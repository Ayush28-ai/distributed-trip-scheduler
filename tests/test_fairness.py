"""Tenant A has 50,000 alerts due at 08:00, tenant B has 20. B must be served in the
first dispatch round, not after A's backlog. Also checks the per-tenant cap and pause."""
import asyncio
from types import SimpleNamespace

from app.db import create_pool
from app.scheduler.dispatcher import Dispatcher
from tests.conftest import reset, seed_runs


class FakeMembership:
    owned = list(range(32))


class RecordingExecutor:
    def __init__(self, capacity: int):
        self._capacity = capacity
        self.runs = []

    def capacity(self) -> int:
        return self._capacity - len(self.runs)

    def submit(self, run) -> None:
        self.runs.append(run)


def _settings(batch: int):
    return SimpleNamespace(node_id="node1", claim_batch=batch, run_lease_s=30,
                           misfire_grace_s=60, tenant_cache_ttl_s=0)


async def _scenario(url: str) -> None:
    pool = await create_pool(url)
    try:
        await reset(pool)
        await seed_runs(pool, "tenant_a", 50_000, cap=10_000)
        await seed_runs(pool, "tenant_b", 20, cap=10_000)

        executor = RecordingExecutor(capacity=100)
        claimed = await Dispatcher(pool, _settings(100), FakeMembership(), executor).tick()
        by_tenant = {}
        for r in executor.runs:
            by_tenant[r["tenant_id"]] = by_tenant.get(r["tenant_id"], 0) + 1
        assert claimed == 100
        assert by_tenant == {"tenant_b": 20, "tenant_a": 80}   # B fully served in round one
    finally:
        await pool.close()


async def _cap_and_pause(url: str) -> None:
    pool = await create_pool(url)
    try:
        await reset(pool)
        await seed_runs(pool, "capped", 500, cap=50)
        await seed_runs(pool, "paused_t", 10)
        await pool.execute("UPDATE tenants SET paused = TRUE WHERE id = 'paused_t'")

        executor = RecordingExecutor(capacity=1000)
        d = Dispatcher(pool, _settings(1000), FakeMembership(), executor)
        await d.tick()
        await d.tick()   # second round: 50 already RUNNING -> cap reached
        tenants = [r["tenant_id"] for r in executor.runs]
        assert tenants.count("capped") == 50
        assert "paused_t" not in tenants
    finally:
        await pool.close()


def test_big_tenant_does_not_starve_small_tenant(test_db_url):
    asyncio.run(_scenario(test_db_url))


def test_tenant_cap_and_pause(test_db_url):
    asyncio.run(_cap_and_pause(test_db_url))
