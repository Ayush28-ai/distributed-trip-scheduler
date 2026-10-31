# Design note: distributed trip job scheduler

## 1. Problem and guarantee

Hundreds of thousands of trip jobs (pickup alerts, rosters, no-show cancels, billing) must
fire on time across a cluster of at least three nodes, survive crashes and pauses, and never
lose a job or duplicate its business effect.

**Exactly-once *execution* is impossible in general.** A node can perform a side effect
(send an SMS) and die before recording that it did. No other node can tell whether the
effect happened, so it must either retry (possible duplicate) or not (possible loss). The
same holds for a node that is merely paused: it cannot know it was paused.

**What this system guarantees: at-least-once execution + exactly-once *effect*.**

- Every firing is a `job_runs` row that is retried until it is committed, so nothing is lost.
- Every firing carries an idempotency key `tenant_id:job_id:scheduled_at`, reused by every
  retry and takeover. The effect store enforces it with a UNIQUE constraint, so a repeated
  attempt is detected and suppressed.
- Every write back to the scheduler is fenced by a monotonically increasing token, so a node
  that lost ownership cannot overwrite a newer owner's result.

## 2. Architecture

```mermaid
flowchart TB
    UI[Streamlit console] --> API
    C[curl / Swagger] --> API
    API[FastAPI API<br/>JWT, tenant scoping, /metrics] --> PG
    subgraph Cluster[Scheduler nodes, identical]
      N1[node1] & N2[node2] & N3[node3]
    end
    PG[(PostgreSQL<br/>jobs, job_runs, partitions,<br/>nodes, effects, dead_letters)]
    N1 & N2 & N3 <--> PG
    N1 & N2 & N3 -- "POST /notifications<br/>Idempotency-Key, Fencing-Token" --> NS[Spring Boot<br/>notification-service]
    NS --> PG
```

Each node runs four async loops:

| Loop | Every | Does |
|---|---|---|
| membership | 2 s | heartbeat; compute desired partitions by rendezvous hashing over live nodes; take/renew those partition leases (10 s); release the rest |
| materializer | 5 s | for cron jobs in owned partitions, create `job_runs` up to 60 s ahead in the tenant's time zone, applying DST and misfire rules |
| dispatcher | 0.1 s | reset expired runs; split free capacity fairly across tenants; claim due runs with `FOR UPDATE SKIP LOCKED`, new fencing token each |
| executor | per run | run handler with max-runtime timeout; renew run leases in one batched UPDATE; fenced commit or fenced retry/dead-letter |

**Why PostgreSQL is the coordinator.** It is the durable job store anyway, its row locks
and `SKIP LOCKED` give contention-free claiming, and its `now()` is a single clock for every
due-time and lease decision (node clocks are never compared). This avoids a second moving
part (ZooKeeper/etcd/Redis) for a 3-node cluster. Trade-off: the database is the scaling
ceiling and a single point of failure (mitigated in production by a managed HA Postgres).

**No single hot lock.** There are 32 partitions (`job_id % 32`). Each node only scans and
claims inside the partitions it leases, and partition leases are 32 independent rows that
change only on membership changes. Rendezvous hashing means a joining or leaving node moves
only its own share of partitions. Partition ownership is for spreading load; *safety does
not depend on it*: if two nodes briefly believe they own a partition, the row-level claim
still hands each run to one node and fencing still rejects stale writes.

## 3. The hardest flow: a node freezes mid-run (SIGSTOP)

```mermaid
sequenceDiagram
    participant A as node A
    participant DB as PostgreSQL
    participant B as node B
    participant NS as notification-service
    A->>DB: claim run 42 (token 7, lease now()+10s)
    Note over A: SIGSTOP, frozen 30 s
    Note over DB: run lease and A's partition lease expire
    B->>DB: heartbeat sees A dead, takes A's partitions
    B->>DB: reset expired run 42, claim it (token 8)
    B->>NS: POST key=acme:42:08:30, token 8
    NS-->>B: 201 created (row inserted)
    B->>DB: commit WHERE id=42 AND token=8 (1 row: SUCCEEDED + effect)
    Note over A: SIGCONT
    A->>NS: POST key=acme:42:08:30, token 7
    NS-->>A: 200 duplicate (UNIQUE key, nothing sent)
    A->>DB: commit WHERE id=42 AND token=7
    DB-->>A: 0 rows: stale_commit_rejected
```

Measured in `tests/failure/sigstop.sh`: the frozen node held 186 in-flight alerts, the other
nodes finished all of them while it was frozen, and on waking all 186 of its writes were
rejected; 600 alerts, 600 notifications, 600 distinct keys.

## 4. Scheduling rules

**Time zones.** Cron is evaluated in the job's zone (default: the tenant's). "0 22 * * *"
for a Bengaluru tenant fires at 16:30 UTC; for a New York tenant at 02:00 or 03:00 UTC
depending on DST. Instants are stored in UTC.

**DST.**
- *Gap* (02:30 does not exist on spring-forward day): fire once at the first valid wall-clock
  minute after the gap (03:00). Several cron times inside one gap collapse into that firing.
- *Overlap* (01:30 happens twice on fall-back day): fire once, on the first occurrence.

**Misfires** (a firing more than 60 s late: cluster down, tenant paused, backlog):
- `FIRE_NOW` runs every late firing as soon as possible (catch-up capped at 100 per job).
- `SKIP` drops late firings and continues from the next on-time one.
- `COALESCE` collapses all late firings of a job into one run (the most recent).
Applied both when cron firings are materialized and when an already-materialized run is
claimed late.

**Retries.** Failure or timeout → `RETRY_WAIT` after `random(0, min(60, 2^(attempt-1)))` s
(full jitter, so retries from one outage do not stampede). After `max_attempts` (default 5)
→ `DEAD` plus a `dead_letters` row; operators inspect and replay it. Replay reuses the run and
its idempotency key, so it cannot duplicate an effect that did happen.

**Fairness.** Each tenant has a concurrency cap (default 50). Each dispatch round, a node's
free capacity is water-filled across tenants with due work: equal shares, and what a small
tenant does not need goes to the others. With 50,000 alerts for tenant A and 20 for tenant B
at 08:00, B's 20 are claimed in the first round (`tests/test_fairness.py`).

## 5. Failure modes

| Failure | User impact | Detection and recovery |
|---|---|---|
| Node crash (kill -9) | Its in-flight alerts are delayed ~10-15 s | Heartbeat stops (dead after 6 s); partition and run leases expire (10 s); survivors take the partitions, reset expired runs, re-claim with new tokens. Measured: 200 in flight, 1,200/1,200 delivered once, 15 s |
| Node frozen (SIGSTOP, GC pause, VM stall) | Same delay; no duplicate | As above; on wake its renewals, commits and failure writes are rejected by fencing token; its duplicate send is suppressed by the idempotency key |
| Crash after side effect, before commit | None | Run is retried by another node; notification-service returns "duplicate"; run commits |
| Handler hangs | That run delayed up to max runtime | `max_runtime_s` timeout → retry with backoff |
| Downstream (notification-service) down | Alerts delayed | Retries with backoff; after max attempts → dead letter, replayable |
| Whole cluster down | Firings late | On restart, misfire policy per job decides fire / skip / coalesce |
| Postgres down | Nothing fires; API returns 5xx | Nodes keep retrying their loops; leases resume after recovery; in production use HA Postgres |
| Network partition: node cut off from DB | Its runs delayed | Same as a freeze: it cannot renew, others take over, its writes are fenced |
| Clock skew between nodes | None | All due-time and lease comparisons use the DB clock `now()` |
| Tenant floods 50k jobs | Other tenants unaffected | Per-tenant cap + fair allocation |
| Duplicate API submission | Two jobs | Not deduplicated at the API (see trade-offs) |

## 6. Security and data handling

- JWT (HS256) with `admin` (all tenants) and `operator` (own tenant only) roles; every
  endpoint is scoped, so an operator cannot read or act on another tenant's jobs.
- Logs are JSON and pass through a PII scrubber; handlers log ids only, never names,
  phone numbers or addresses. The Spring sender logs tenant, trip id, token and node only.
- Secrets come from environment variables (`.env`, not committed).

## 7. Trade-offs and what I would do next

- **Postgres as coordinator** instead of etcd/ZooKeeper: fewer moving parts, one clock;
  the DB is the ceiling. Next: read replicas for the API, partitioned `job_runs` tables,
  archiving finished runs.
- **Soft per-tenant cap across nodes:** two nodes may read the same running count at the
  same instant and briefly exceed the cap by a few runs. A hard cap would need a counter row
  per tenant, which is exactly the hot lock we avoided.
- **Tenant config cached for 2 s:** pause takes effect within 2 s.
- **Fixed 32 partitions:** enough for tens of nodes; changing the count needs a migration.
- **Effect store per side-effect owner:** local effects use the `effects` table inside the
  commit transaction; external effects (notifications) dedupe in their own service. Any new
  external integration must accept an idempotency key.
- **Python for the scheduler, Spring Boot for the notification service:** delivery speed for
  the core in the language I know best; the side-effect service is in the preferred stack and
  shows idempotency across a service boundary.
- **Not built:** API-level request idempotency, Prometheus/Grafana containers (metrics are
  exposed), multi-region.
