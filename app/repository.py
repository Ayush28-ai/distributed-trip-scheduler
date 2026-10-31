"""All SQL lives here. Every time comparison uses the database clock (now()),
so node clocks never decide when a job is due or whether a lease is valid.

`db` can be an asyncpg Pool or Connection (both have fetch/execute)."""
from datetime import datetime

# ---------------------------------------------------------------- tenants

async def upsert_tenant(db, tenant_id: str, timezone: str, max_concurrency: int):
    return await db.fetchrow(
        """INSERT INTO tenants (id, timezone, max_concurrency) VALUES ($1, $2, $3)
           ON CONFLICT (id) DO UPDATE SET timezone = EXCLUDED.timezone,
                                          max_concurrency = EXCLUDED.max_concurrency
           RETURNING *""", tenant_id, timezone, max_concurrency)


async def get_tenant(db, tenant_id: str):
    return await db.fetchrow("SELECT * FROM tenants WHERE id = $1", tenant_id)


async def list_tenants(db, tenant_id: str | None = None):
    return await db.fetch(
        "SELECT * FROM tenants WHERE ($1::text IS NULL OR id = $1) ORDER BY id", tenant_id)


async def set_tenant_paused(db, tenant_id: str, paused: bool) -> bool:
    res = await db.execute("UPDATE tenants SET paused = $2 WHERE id = $1", tenant_id, paused)
    return res == "UPDATE 1"


async def tenant_configs(db) -> dict[str, tuple[bool, int]]:
    rows = await db.fetch("SELECT id, paused, max_concurrency FROM tenants")
    return {r["id"]: (r["paused"], r["max_concurrency"]) for r in rows}


# ---------------------------------------------------------------- jobs + runs

async def db_now(db) -> datetime:
    return await db.fetchval("SELECT now()")


async def insert_job(db, *, tenant_id, name, job_type, cron, run_at, timezone, priority,
                     max_runtime_s, max_attempts, misfire_policy, payload, next_fire_at):
    return await db.fetchrow(
        """INSERT INTO jobs (tenant_id, name, job_type, cron, run_at, timezone, priority,
                             max_runtime_s, max_attempts, misfire_policy, payload, next_fire_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *""",
        tenant_id, name, job_type, cron, run_at, timezone, priority,
        max_runtime_s, max_attempts, misfire_policy, payload, next_fire_at)


async def insert_run(db, *, job_id, tenant_id, partition, scheduled_at, priority, misfire_policy):
    return await db.fetchval(
        """INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at,
                                 priority, misfire_policy)
           VALUES ($1,$2,$3,$4,$4,$5,$6)
           ON CONFLICT (job_id, scheduled_at) DO NOTHING RETURNING id""",
        job_id, tenant_id, partition, scheduled_at, priority, misfire_policy)


async def get_job(db, job_id: int):
    return await db.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)


async def list_jobs(db, tenant_id: str | None, limit: int = 100):
    return await db.fetch(
        """SELECT * FROM jobs WHERE ($1::text IS NULL OR tenant_id = $1)
           ORDER BY id DESC LIMIT $2""", tenant_id, limit)


async def trigger_now(db, job, partition: int) -> int:
    """Manual trigger: a new run scheduled at the DB's current instant."""
    return await db.fetchval(
        """INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at,
                                 priority, misfire_policy)
           VALUES ($1,$2,$3,clock_timestamp(),clock_timestamp(),$4,$5) RETURNING id""",
        job["id"], job["tenant_id"], partition, job["priority"], job["misfire_policy"])


async def list_runs(db, tenant_id: str | None, status: str | None, limit: int = 200):
    return await db.fetch(
        """SELECT r.*, j.name, j.job_type FROM job_runs r JOIN jobs j ON j.id = r.job_id
           WHERE ($1::text IS NULL OR r.tenant_id = $1) AND ($2::text IS NULL OR r.status = $2)
           ORDER BY r.id DESC LIMIT $3""", tenant_id, status, limit)


# ---------------------------------------------------------------- membership

async def heartbeat(db, node_id: str) -> None:
    await db.execute(
        """INSERT INTO nodes (node_id, last_heartbeat) VALUES ($1, now())
           ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = now()""", node_id)


async def live_nodes(db, dead_after_s: float) -> list[str]:
    rows = await db.fetch(
        "SELECT node_id FROM nodes WHERE last_heartbeat > now() - make_interval(secs => $1)",
        float(dead_after_s))
    return [r["node_id"] for r in rows]


async def acquire_partitions(db, node_id: str, desired: list[int], lease_s: float) -> list[int]:
    """Take or renew leases on desired partitions that are free, expired, or already ours."""
    rows = await db.fetch(
        """UPDATE partitions
              SET epoch = CASE WHEN owner_node = $1 THEN epoch ELSE epoch + 1 END,
                  owner_node = $1,
                  lease_until = now() + make_interval(secs => $3)
            WHERE id = ANY($2::int[])
              AND (owner_node IS NULL OR owner_node = $1 OR lease_until < now())
        RETURNING id""", node_id, desired, float(lease_s))
    return sorted(r["id"] for r in rows)


async def release_partitions(db, node_id: str, keep: list[int]) -> None:
    await db.execute(
        """UPDATE partitions SET owner_node = NULL, lease_until = NULL
            WHERE owner_node = $1 AND NOT (id = ANY($2::int[]))""", node_id, keep)


async def cluster_view(db):
    return await db.fetch(
        """SELECT n.node_id,
                  EXTRACT(EPOCH FROM now() - n.last_heartbeat)::float AS heartbeat_age_s,
                  COALESCE(array_agg(p.id ORDER BY p.id) FILTER (WHERE p.id IS NOT NULL), '{}') AS partitions
             FROM nodes n
             LEFT JOIN partitions p ON p.owner_node = n.node_id AND p.lease_until > now()
            GROUP BY n.node_id, n.last_heartbeat ORDER BY n.node_id""")


# ---------------------------------------------------------------- dispatch

async def reset_expired(db, owned: list[int]) -> int:
    """Runs whose lease expired (owner crashed or froze) become claimable again.
    The old owner keeps its old fencing token, so anything it writes later is rejected."""
    res = await db.execute(
        """UPDATE job_runs SET status = 'RETRY_WAIT', next_attempt_at = now(), owner_node = NULL,
                               last_error = 'lease expired: owner lost'
            WHERE partition = ANY($1::int[]) AND status = 'RUNNING' AND lease_until < now()""",
        owned)
    return int(res.split()[-1])


async def due_by_tenant(db, owned: list[int]) -> dict[str, int]:
    rows = await db.fetch(
        """SELECT tenant_id, count(*) AS n FROM job_runs
            WHERE status IN ('SCHEDULED','RETRY_WAIT') AND next_attempt_at <= now()
              AND partition = ANY($1::int[])
            GROUP BY tenant_id""", owned)
    return {r["tenant_id"]: r["n"] for r in rows}


async def running_by_tenant(db) -> dict[str, int]:
    rows = await db.fetch(
        """SELECT tenant_id, count(*) AS n FROM job_runs
            WHERE status = 'RUNNING' AND lease_until > now() GROUP BY tenant_id""")
    return {r["tenant_id"]: r["n"] for r in rows}


async def claim(db, owned: list[int], tenant_id: str, limit: int, node_id: str, lease_s: float):
    """Atomically claim up to `limit` due runs of one tenant. SKIP LOCKED means
    concurrent claimers never block on or double-claim the same row. Each claim
    takes a new fencing token from a global sequence."""
    return await db.fetch(
        """WITH c AS (
               SELECT id, fire_lag_ms AS prev_lag FROM job_runs
                WHERE tenant_id = $1 AND partition = ANY($2::int[])
                  AND status IN ('SCHEDULED','RETRY_WAIT') AND next_attempt_at <= now()
                ORDER BY priority DESC, next_attempt_at
                LIMIT $3
                FOR UPDATE SKIP LOCKED)
           UPDATE job_runs r
              SET status = 'RUNNING', owner_node = $4,
                  lease_until = now() + make_interval(secs => $5),
                  fencing_token = nextval('fencing_seq'),
                  attempt = r.attempt + 1,
                  started_at = now(),
                  fire_lag_ms = COALESCE(r.fire_lag_ms,
                                         EXTRACT(EPOCH FROM (now() - r.scheduled_at)) * 1000)
             FROM c, jobs j
            WHERE r.id = c.id AND j.id = r.job_id
        RETURNING r.id, r.job_id, r.tenant_id, r.scheduled_at, r.fencing_token, r.attempt,
                  r.fire_lag_ms, r.misfire_policy, (c.prev_lag IS NULL) AS first_claim,
                  j.job_type, j.payload, j.max_runtime_s, j.max_attempts""",
        tenant_id, owned, limit, node_id, float(lease_s))


async def has_newer_due(db, job_id: int, scheduled_at: datetime) -> bool:
    return await db.fetchval(
        """SELECT EXISTS (SELECT 1 FROM job_runs WHERE job_id = $1 AND scheduled_at > $2
                          AND scheduled_at <= now() AND status <> 'SKIPPED')""", job_id, scheduled_at)


async def skip_if_token(db, run_id: int, token: int, reason: str) -> bool:
    res = await db.execute(
        """UPDATE job_runs SET status = 'SKIPPED', finished_at = now(), lease_until = NULL,
                               last_error = $3
            WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'""", run_id, token, reason)
    return res == "UPDATE 1"


# ---------------------------------------------------------------- execution (fenced writes)

async def renew_leases(db, run_ids: list[int], tokens: list[int], lease_s: float) -> set[int]:
    rows = await db.fetch(
        """UPDATE job_runs r SET lease_until = now() + make_interval(secs => $3)
             FROM unnest($1::bigint[], $2::bigint[]) AS x(id, tok)
            WHERE r.id = x.id AND r.fencing_token = x.tok AND r.status = 'RUNNING'
        RETURNING r.id""", run_ids, tokens, float(lease_s))
    return {r["id"] for r in rows}


async def commit_if_token(pool, run_id: int, token: int, idem_key: str, tenant_id: str,
                          job_type: str) -> bool:
    """Mark SUCCEEDED and record the effect in ONE transaction, only if we still hold
    the current fencing token. A stale owner updates 0 rows and writes nothing."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            res = await conn.execute(
                """UPDATE job_runs SET status = 'SUCCEEDED', finished_at = now(),
                                       lease_until = NULL, last_error = NULL
                    WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'""", run_id, token)
            if res != "UPDATE 1":
                return False
            await conn.execute(
                """INSERT INTO effects (idempotency_key, tenant_id, run_id, fencing_token, job_type)
                   VALUES ($1,$2,$3,$4,$5) ON CONFLICT (idempotency_key) DO NOTHING""",
                idem_key, tenant_id, run_id, token, job_type)
            return True


async def fail_if_token(pool, run_id: int, token: int, error: str,
                        retry_delay_s: float | None) -> bool:
    """retry_delay_s=None -> DEAD + dead letter; otherwise RETRY_WAIT after the delay."""
    error = error[:500]
    async with pool.acquire() as conn:
        async with conn.transaction():
            if retry_delay_s is not None:
                res = await conn.execute(
                    """UPDATE job_runs SET status = 'RETRY_WAIT', owner_node = NULL, lease_until = NULL,
                                           next_attempt_at = now() + make_interval(secs => $4),
                                           last_error = $3
                        WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'""",
                    run_id, token, error, float(retry_delay_s))
                return res == "UPDATE 1"
            row = await conn.fetchrow(
                """UPDATE job_runs SET status = 'DEAD', owner_node = NULL, lease_until = NULL,
                                       finished_at = now(), last_error = $3
                    WHERE id = $1 AND fencing_token = $2 AND status = 'RUNNING'
                RETURNING job_id, tenant_id, attempt""", run_id, token, error)
            if row is None:
                return False
            await conn.execute(
                """INSERT INTO dead_letters (run_id, job_id, tenant_id, attempts, last_error)
                   VALUES ($1,$2,$3,$4,$5)
                   ON CONFLICT (run_id) DO UPDATE SET attempts = EXCLUDED.attempts,
                       last_error = EXCLUDED.last_error, failed_at = now(), replayed_at = NULL""",
                run_id, row["job_id"], row["tenant_id"], row["attempt"], error)
            return True


# ---------------------------------------------------------------- materializer

async def lock_due_cron_jobs(conn, owned: list[int], num_partitions: int, horizon_s: float,
                             limit: int = 200):
    """Call inside a transaction. Locks cron jobs whose cursor is within the horizon."""
    return await conn.fetch(
        """SELECT j.* FROM jobs j JOIN tenants t ON t.id = j.tenant_id
            WHERE j.cron IS NOT NULL AND j.active AND NOT t.paused
              AND j.next_fire_at <= now() + make_interval(secs => $1)
              AND (j.id % $2) = ANY($3::int[])
            ORDER BY j.next_fire_at LIMIT $4
            FOR UPDATE OF j SKIP LOCKED""", float(horizon_s), num_partitions, owned, limit)


async def advance_cursor(conn, job_id: int, next_fire_at: datetime) -> None:
    await conn.execute("UPDATE jobs SET next_fire_at = $2 WHERE id = $1", job_id, next_fire_at)


# ---------------------------------------------------------------- dead letters + stats

async def list_dead_letters(db, tenant_id: str | None, include_replayed: bool = False):
    return await db.fetch(
        """SELECT d.*, j.name, j.job_type FROM dead_letters d JOIN jobs j ON j.id = d.job_id
            WHERE ($1::text IS NULL OR d.tenant_id = $1) AND ($2 OR d.replayed_at IS NULL)
            ORDER BY d.id DESC LIMIT 200""", tenant_id, include_replayed)


async def get_dead_letter(db, dl_id: int):
    return await db.fetchrow("SELECT * FROM dead_letters WHERE id = $1", dl_id)


async def replay_dead_letter(db, dl_id: int) -> int | None:
    """Re-queue a dead run. It keeps its idempotency key, so a replay never duplicates an effect."""
    return await db.fetchval(
        """WITH d AS (UPDATE dead_letters SET replayed_at = now()
                       WHERE id = $1 AND replayed_at IS NULL RETURNING run_id)
           UPDATE job_runs r SET status = 'SCHEDULED', attempt = 0, next_attempt_at = now(),
                                 finished_at = NULL, last_error = NULL
             FROM d WHERE r.id = d.run_id AND r.status = 'DEAD'
         RETURNING r.id""", dl_id)


async def stats(db, tenant_id: str | None = None) -> dict:
    counts = await db.fetch(
        """SELECT status, count(*) AS n FROM job_runs
            WHERE ($1::text IS NULL OR tenant_id = $1) GROUP BY status""", tenant_id)
    lag = await db.fetchrow(
        """SELECT count(*) AS n,
                  percentile_cont(0.5)  WITHIN GROUP (ORDER BY fire_lag_ms) AS p50,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY fire_lag_ms) AS p95,
                  percentile_cont(0.99) WITHIN GROUP (ORDER BY fire_lag_ms) AS p99,
                  max(fire_lag_ms) AS max
             FROM job_runs
            WHERE started_at > now() - interval '10 minutes' AND fire_lag_ms IS NOT NULL
              AND ($1::text IS NULL OR tenant_id = $1)""", tenant_id)
    done_1m = await db.fetchval(
        """SELECT count(*) FROM job_runs WHERE status = 'SUCCEEDED'
              AND finished_at > now() - interval '60 seconds'
              AND ($1::text IS NULL OR tenant_id = $1)""", tenant_id)
    dead_pending = await db.fetchval(
        """SELECT count(*) FROM dead_letters WHERE replayed_at IS NULL
              AND ($1::text IS NULL OR tenant_id = $1)""", tenant_id)
    queue = await db.fetchrow(
        """SELECT count(*) AS depth,
                  COALESCE(EXTRACT(EPOCH FROM now() - min(next_attempt_at)), 0)::float AS oldest_s
             FROM job_runs
            WHERE status IN ('SCHEDULED','RETRY_WAIT') AND next_attempt_at <= now()
              AND ($1::text IS NULL OR tenant_id = $1)""", tenant_id)
    errors = await db.fetchrow(
        """SELECT count(*) AS started,
                  count(*) FILTER (WHERE last_error IS NOT NULL AND status <> 'SKIPPED') AS with_error
             FROM job_runs
            WHERE started_at > now() - interval '10 minutes'
              AND ($1::text IS NULL OR tenant_id = $1)""", tenant_id)
    by_status = {r["status"]: r["n"] for r in counts}
    return {
        "runs_by_status": by_status,
        "running": by_status.get("RUNNING", 0),
        "retrying": by_status.get("RETRY_WAIT", 0),
        "dead": by_status.get("DEAD", 0),
        "dead_letters_pending": dead_pending,
        "queue_depth": queue["depth"],                      # due now, not yet claimed
        "oldest_due_age_s": round(queue["oldest_s"], 1),
        "error_rate_last_10m": round(errors["with_error"] / errors["started"], 4) if errors["started"] else 0.0,
        "succeeded_last_60s": done_1m,
        "fire_lag_ms_last_10m": {k: (round(lag[k], 1) if lag[k] is not None else None)
                                 for k in ("p50", "p95", "p99", "max")} | {"samples": lag["n"]},
    }
