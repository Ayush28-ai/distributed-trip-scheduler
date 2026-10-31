"""Benchmark: N one-off jobs due inside a W-second window, executed by the running nodes.
Reports fire-lag p50/p95/p99 (claim time - scheduled time, DB clock) and throughput.

    docker compose up -d --build
    python bench/load_100k.py                          # 100,000 jobs in 10 minutes
    python bench/load_100k.py --count 10000 --window 60   # quick smoke run
"""
import argparse
import asyncio
import os
import platform
import time
from datetime import datetime
from pathlib import Path

import asyncpg

DB = os.getenv("BENCH_DATABASE_URL", "postgresql://scheduler:scheduler@localhost:5432/scheduler")


def machine() -> str:
    mem = ""
    try:
        kb = int(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).split()[1])
        mem = f", {kb / 1024 / 1024:.1f} GB RAM"
    except Exception:
        pass
    return f"{platform.platform()}, {os.cpu_count()} CPUs{mem}, Python {platform.python_version()}"


async def seed(conn, count: int, window: float, lead: float, tenants: int) -> None:
    await conn.execute("DELETE FROM effects WHERE tenant_id LIKE 'bench_%'")
    await conn.execute("DELETE FROM jobs WHERE tenant_id LIKE 'bench_%'")
    per = count // tenants
    for t in range(tenants):
        tid = f"bench_{t}"
        await conn.execute(
            """INSERT INTO tenants (id, max_concurrency) VALUES ($1, 5000)
               ON CONFLICT (id) DO UPDATE SET paused = FALSE, max_concurrency = 5000""", tid)
        n = per + (count - per * tenants if t == 0 else 0)
        await conn.execute(
            """WITH j AS (
                   INSERT INTO jobs (tenant_id, name, job_type, run_at)
                   SELECT $1, 'bench-' || g, 'noop',
                          date_trunc('milliseconds', now() + make_interval(secs => $3 + random() * $4))
                     FROM generate_series(1, $2) g
                   RETURNING id, tenant_id, run_at, priority, misfire_policy)
               INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at,
                                     priority, misfire_policy)
               SELECT id, tenant_id, id % 32, run_at, run_at, priority, misfire_policy FROM j""",
            tid, n, float(lead), float(window))


async def main(args) -> None:
    conn = await asyncpg.connect(args.db)
    try:
        nodes = await conn.fetchval(
            "SELECT count(*) FROM nodes WHERE last_heartbeat > now() - interval '6 seconds'")
        print(f"live nodes: {nodes}")
        t0 = time.time()
        await seed(conn, args.count, args.window, args.lead, args.tenants)
        print(f"seeded {args.count:,} jobs over {args.window:.0f}s (first due in {args.lead:.0f}s) "
              f"in {time.time() - t0:.1f}s")

        deadline = time.time() + args.lead + args.window + args.timeout
        while time.time() < deadline:
            row = await conn.fetchrow(
                """SELECT count(*) FILTER (WHERE status = 'SUCCEEDED') AS done,
                          count(*) FILTER (WHERE status = 'RUNNING') AS running,
                          count(*) FILTER (WHERE status IN ('SCHEDULED','RETRY_WAIT')
                                           AND next_attempt_at <= now()) AS due_waiting
                     FROM job_runs WHERE tenant_id LIKE 'bench_%'""")
            print(f"  done {row['done']:>7,}/{args.count:,}  running {row['running']:>4}  "
                  f"due-not-started {row['due_waiting']:>5}", flush=True)
            if row["done"] >= args.count:
                break
            await asyncio.sleep(5)

        r = await conn.fetchrow(
            """SELECT count(*) AS n,
                      percentile_cont(0.50) WITHIN GROUP (ORDER BY fire_lag_ms) AS p50,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY fire_lag_ms) AS p95,
                      percentile_cont(0.99) WITHIN GROUP (ORDER BY fire_lag_ms) AS p99,
                      max(fire_lag_ms) AS max,
                      count(*) FILTER (WHERE fire_lag_ms > 2000) AS over_2s,
                      EXTRACT(EPOCH FROM max(finished_at) - min(scheduled_at)) AS span_s,
                      count(*) FILTER (WHERE status = 'SUCCEEDED') AS ok
                 FROM job_runs WHERE tenant_id LIKE 'bench_%'""")
        dup = await conn.fetchval(
            """SELECT count(*) - count(DISTINCT idempotency_key) FROM effects
                WHERE tenant_id LIKE 'bench_%'""")
    finally:
        await conn.close()

    peak = args.count / args.window
    lines = [
        f"## Benchmark {datetime.now():%Y-%m-%d %H:%M}",
        "",
        f"- Machine: {machine()}",
        f"- Live scheduler nodes: {nodes}",
        f"- Load: {args.count:,} jobs due uniformly over {args.window:.0f}s "
        f"(~{peak:.0f} jobs/s offered), {args.tenants} tenants",
        "",
        "| metric | value |",
        "|---|---|",
        f"| succeeded | {r['ok']:,} / {args.count:,} |",
        f"| fire lag p50 | {r['p50']:.0f} ms |",
        f"| fire lag p95 | {r['p95']:.0f} ms |",
        f"| fire lag p99 | {r['p99']:.0f} ms |",
        f"| fire lag max | {r['max']:.0f} ms |",
        f"| runs with lag > 2 s | {r['over_2s']:,} |",
        f"| throughput (completed / span) | {r['n'] / float(r['span_s']):.0f} jobs/s |",
        f"| duplicate effects | {dup} |",
        f"| p99 < 2 s target | {'PASS' if r['p99'] < 2000 else 'FAIL'} |",
    ]
    report = "\n".join(lines)
    print("\n" + report)
    out = Path(__file__).parent / "results"
    out.mkdir(exist_ok=True)
    path = out / f"bench_{datetime.now():%Y%m%d_%H%M%S}.md"
    path.write_text(report + "\n")
    print(f"\nsaved {path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=100_000)
    p.add_argument("--window", type=float, default=600, help="seconds over which jobs are due")
    p.add_argument("--lead", type=float, default=20, help="seconds before the first job is due")
    p.add_argument("--tenants", type=int, default=10)
    p.add_argument("--timeout", type=float, default=180, help="extra seconds to wait after the window")
    p.add_argument("--db", default=DB)
    asyncio.run(main(p.parse_args()))
