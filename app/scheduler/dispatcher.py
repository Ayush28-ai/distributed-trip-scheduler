"""Claims due runs in owned partitions and hands them to the executor.

Per loop:
1. Reset expired RUNNING runs in our partitions (takeover after a crash/freeze).
2. Count due runs per tenant, subtract each tenant's running count from its cap.
3. Split our free capacity fairly across tenants (core/fairness.py).
4. Claim per tenant with FOR UPDATE SKIP LOCKED; each claim gets a new fencing token.
5. Apply the misfire policy to late runs, then submit the rest to the executor.

Tenant config (paused, cap) is cached for TENANT_CACHE_TTL_S: pause takes effect
within that bound. The per-tenant cap is soft across nodes (two nodes may read the
same running count in the same instant); documented trade-off.
"""
import asyncio
import logging
import time

from app import repository as repo
from app.core.fairness import fair_allocate, tenant_demand
from app.core.misfire import decide_late_run
from app.log_config import log_event
from app.metrics import CLAIMED, FIRE_LAG, MISFIRE_SKIPPED, RECLAIMED

log = logging.getLogger("dispatcher")


class TenantCache:
    def __init__(self, pool, ttl_s: float):
        self.pool, self.ttl = pool, ttl_s
        self._data: dict[str, tuple[bool, int]] = {}
        self._loaded = 0.0

    async def get(self) -> dict[str, tuple[bool, int]]:
        if time.monotonic() - self._loaded > self.ttl:
            self._data = await repo.tenant_configs(self.pool)
            self._loaded = time.monotonic()
        return self._data


class Dispatcher:
    def __init__(self, pool, settings, membership, executor):
        self.pool = pool
        self.s = settings
        self.membership = membership
        self.executor = executor
        self.tenants = TenantCache(pool, settings.tenant_cache_ttl_s)
        self._last_reset = 0.0

    async def tick(self) -> int:
        owned = self.membership.owned
        if not owned:
            return 0
        if time.monotonic() - self._last_reset > 1.0:
            n = await repo.reset_expired(self.pool, owned)
            self._last_reset = time.monotonic()
            if n:
                RECLAIMED.inc(n)
                log_event(log, "expired_runs_reset_for_takeover", count=n, node=self.s.node_id)

        capacity = min(self.executor.capacity(), self.s.claim_batch)
        if capacity <= 0:
            return 0
        due = await repo.due_by_tenant(self.pool, owned)
        if not due:
            return 0
        cfg = await self.tenants.get()
        due = {t: n for t, n in due.items() if t in cfg and not cfg[t][0]}  # drop paused
        if not due:
            return 0
        running = await repo.running_by_tenant(self.pool)
        caps = {t: cfg[t][1] for t in due}
        allocation = fair_allocate(capacity, tenant_demand(due, running, caps))

        claimed = 0
        for tenant, n in allocation.items():
            runs = await repo.claim(self.pool, owned, tenant, n, self.s.node_id, self.s.run_lease_s)
            for run in runs:
                claimed += 1
                CLAIMED.inc()
                if run["first_claim"]:
                    FIRE_LAG.observe(max(0.0, run["fire_lag_ms"] / 1000))
                if await self._skip_if_misfired(run):
                    continue
                self.executor.submit(run)
        return claimed

    async def _skip_if_misfired(self, run) -> bool:
        lag_s = (run["fire_lag_ms"] or 0) / 1000
        if not run["first_claim"] or lag_s <= self.s.misfire_grace_s:
            return False
        newer = await repo.has_newer_due(self.pool, run["job_id"], run["scheduled_at"])
        if decide_late_run(run["misfire_policy"], lag_s, self.s.misfire_grace_s, newer) == "SKIP":
            await repo.skip_if_token(self.pool, run["id"], run["fencing_token"],
                                     f"misfire {run['misfire_policy']}: {lag_s:.0f}s late")
            MISFIRE_SKIPPED.inc()
            return True
        return False

    async def run(self) -> None:
        while True:
            try:
                claimed = await self.tick()
            except Exception:
                log.exception("dispatcher_tick_failed")
                claimed = 0
            await asyncio.sleep(0.01 if claimed else self.s.poll_interval_s)
