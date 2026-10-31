# Cloudstream distributed trip scheduler

A distributed job scheduler for corporate trip operations: pickup alerts, nightly rosters,
no-show cancels and billing close, running on a cluster of three nodes. Jobs survive node
crashes and freezes, and their business effects happen exactly once.

**Guarantee:** at-least-once execution + exactly-once effect, via leases, fencing tokens and
idempotency keys. Why exactly-once execution is impossible, and how this works instead:
[docs/design.md](docs/design.md).

## Run it

```bash
cp .env.example .env
docker compose up --build
```

| URL | What |
|---|---|
| http://localhost:8501 | Streamlit console (login `admin` / `admin123`) |
| http://localhost:8000/docs | FastAPI Swagger |
| http://localhost:8000/metrics | Cluster Prometheus metrics |
| http://localhost:9101/metrics (9102, 9103) | Per-node Prometheus metrics |
| http://localhost:8080/notifications/stats?tenantId=acme | Spring notification-service |

One command starts 7 containers: `postgres`, `api`, `node1`, `node2`, `node3`,
`notification-service` (Spring Boot) and `ui` (Streamlit). The schema is created from
`migrations/schema.sql` on first start; after changing it run `docker compose down -v`.

## Architecture

```
Streamlit / curl ─► FastAPI API ─► PostgreSQL ◄── node1, node2, node3 (Python)
                                       ▲                 │ POST /notifications
                                       └── Spring Boot ◄─┘ (Idempotency-Key)
```

- **Partitions, no hot lock:** 32 partitions (`job_id % 32`), leased to live nodes by
  rendezvous hashing; nodes only scan their own partitions.
- **Leases:** a claimed run is leased for 10 s and renewed while it executes.
- **Fencing:** every claim takes a new token from a global sequence; every write-back is
  `WHERE id = $run AND fencing_token = $mine`, so a node that lost ownership writes nothing.
- **Idempotency:** key `tenant:job:scheduled_at`, unique in the effect store.
- **Fairness:** per-tenant concurrency cap + water-filling of each node's free capacity.
- **Retries:** exponential backoff with full jitter, then dead letter + replay.
- **Misfires:** `FIRE_NOW`, `SKIP`, `COALESCE` per job.
- **Time zones + DST:** cron evaluated in the tenant's zone; explicit gap/overlap rules.
- **Clock skew:** all due-time and lease checks use the DB clock.

## API (all need `Authorization: Bearer <token>` except login, health, metrics)

```bash
TOKEN=$(curl -s -X POST localhost:8000/auth/token -H 'content-type: application/json' \
  -d '{"username":"admin","password":"admin123"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
H=(-H "Authorization: Bearer $TOKEN" -H 'content-type: application/json')

curl -s -X POST localhost:8000/tenants "${H[@]}" -d '{"id":"acme","timezone":"Asia/Kolkata","max_concurrency":50}'
# one-off pickup alert
curl -s -X POST localhost:8000/jobs "${H[@]}" -d '{"tenant_id":"acme","name":"pickup T-100","job_type":"pickup_alert",
  "run_at":"2026-11-01T08:30:00+05:30","payload":{"trip_id":"T-100"}}'
# nightly roster at 10 PM tenant time
curl -s -X POST localhost:8000/jobs "${H[@]}" -d '{"tenant_id":"acme","name":"roster","job_type":"roster","cron":"0 22 * * *"}'
curl -s localhost:8000/jobs/2/next-fire-times?n=5 "${H[@]}"
curl -s -X POST localhost:8000/jobs/2/trigger "${H[@]}"
curl -s -X POST localhost:8000/tenants/acme/pause "${H[@]}"
curl -s -X POST localhost:8000/tenants/acme/resume "${H[@]}"
curl -s localhost:8000/dead-letters "${H[@]}"
curl -s -X POST localhost:8000/dead-letters/1/replay "${H[@]}"
curl -s localhost:8000/stats "${H[@]}"
curl -s localhost:8000/cluster "${H[@]}"
```

Roles: `admin` sees all tenants; operators (`acme_ops`/`acme123`, `globex_ops`/`globex123`)
see and act on their own tenant only. Job types: `pickup_alert` (calls the Spring service),
`roster`, `billing`, `no_show`, `noop`, `flaky` (fails with `payload.fail_rate`, for
retry/dead-letter demos).

## Tests

```bash
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
docker compose up -d postgres          # DB tests use a separate scheduler_test database
pytest                                 # unit + concurrency + fencing + fairness

docker compose up -d --build           # failure tests need the full stack
./tests/failure/kill9.sh               # kill -9 node1 during 1,200 pickup alerts
./tests/failure/sigstop.sh             # freeze node2 for 30 s with SIGSTOP
```

| Test | Proves |
|---|---|
| `test_core.py` | DST gap/overlap, time zones, misfire policies, backoff, partitioning, fair allocation (pure, no DB) |
| `test_concurrency.py` | 3 dispatchers racing for 10,000 runs (all claiming every partition): each run claimed once, 10,000 effects, 0 duplicates |
| `test_fencing.py` | stale owner's renew/commit/fail all rejected after takeover; retry → dead letter → replay |
| `test_fairness.py` | 50,000 vs 20 due runs: the small tenant is fully served in round one; tenant cap and pause honoured |
| `NotificationServiceTest` (Java) | repeated idempotency key sends nothing (runs during the Docker build) |
| `kill9.sh` | crash during 1,200 alerts: nothing lost, nothing sent twice |
| `sigstop.sh` | 30 s freeze: stale writes rejected, every alert sent once |

## Benchmark

```bash
docker compose up -d --build
python bench/load_100k.py              # 100,000 jobs due within 10 minutes, 3 nodes
```

Fire lag = time a node claimed the run minus its scheduled time, on the DB clock.
The script prints a markdown report and saves it under `bench/results/`.

<!-- Paste your bench/results/*.md output here -->
| metric | value |
|---|---|
| machine | _fill in_ |
| jobs / window / nodes | 100,000 / 600 s / 3 |
| fire lag p50 | _fill in_ |
| fire lag p99 | _fill in_ |
| throughput | _fill in_ |
| duplicate effects | _fill in_ |

## Assumptions

- A "firing" is identified by (job, scheduled instant); manual triggers are new firings.
- Fire lag target is measured to claim (start of execution), not to completion.
- Late = more than 60 s after the scheduled time (`MISFIRE_GRACE_S`).
- Pause stops dispatching within 2 s (tenant cache TTL); runs stay queued and go through
  the misfire policy on resume.
- The tenant concurrency cap is enforced softly across nodes (may exceed by a few briefly).

## Trade-offs

See [docs/design.md](docs/design.md#7-trade-offs-and-what-i-would-do-next). In short:
Postgres as the only coordinator (simple, one clock, DB is the ceiling); Python core for
delivery speed, Spring Boot for the side-effect service; a soft tenant cap to avoid a hot
counter row; fixed 32 partitions.

## Layout

```
app/core/        pure logic: schedule (cron + DST), misfire, backoff, partitioning, fairness
app/scheduler/   node loops: membership, materializer, dispatcher, executor
app/repository.py  all SQL (claim, fenced commit, reclaim, dead letters)
app/handlers/    job types (polymorphic JobHandler)
app/api/         FastAPI routers;  app/auth.py JWT;  app/metrics.py Prometheus
notification-service/  Spring Boot idempotent notification sink
ui/              Streamlit console
tests/           unit, concurrency, fencing, fairness, failure scripts
bench/           100k benchmark
docs/design.md   design note, diagrams, failure-mode table
```
