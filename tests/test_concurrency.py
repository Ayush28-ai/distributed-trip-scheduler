"""3 dispatchers race for the same 10,000 runs. Worst case on purpose: every dispatcher
claims from ALL partitions. Row-level claims must still give each run to exactly one
node, and every run must produce exactly one effect."""
import asyncio

from app import repository as repo
from app.db import create_pool
from app.handlers.base import idempotency_key
from tests.conftest import reset, seed_runs

N_RUNS = 10_000
ALL = list(range(32))


async def _worker(pool, node: str, claimed: list[int]) -> None:
    idle = 0
    while idle < 3:
        runs = await repo.claim(pool, ALL, "acme", 100, node, 30)
        if not runs:
            idle += 1
            await asyncio.sleep(0.05)
            continue
        idle = 0
        for r in runs:
            claimed.append(r["id"])
            key = idempotency_key(r["tenant_id"], r["job_id"], r["scheduled_at"])
            assert await repo.commit_if_token(pool, r["id"], r["fencing_token"], key,
                                              r["tenant_id"], r["job_type"])


async def _scenario(url: str) -> None:
    pool = await create_pool(url, max_size=12)
    try:
        await reset(pool)
        await seed_runs(pool, "acme", N_RUNS)
        claims = {n: [] for n in ("node1", "node2", "node3")}
        await asyncio.gather(*(_worker(pool, n, c) for n, c in claims.items()))

        all_claims = [i for c in claims.values() for i in c]
        assert len(all_claims) == N_RUNS                     # every run claimed...
        assert len(set(all_claims)) == N_RUNS                # ...by exactly one node
        assert all(len(c) > 0 for c in claims.values())      # all three did work
        effects = await pool.fetchrow("SELECT count(*) n, count(DISTINCT idempotency_key) d FROM effects")
        assert effects["n"] == effects["d"] == N_RUNS        # zero duplicate effects
        assert await pool.fetchval("SELECT count(*) FROM job_runs WHERE status = 'SUCCEEDED'") == N_RUNS
    finally:
        await pool.close()


def test_three_dispatchers_no_double_claims_no_duplicate_effects(test_db_url):
    asyncio.run(_scenario(test_db_url))
