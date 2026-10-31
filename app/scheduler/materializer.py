"""Turns cron jobs into concrete job_runs a short horizon ahead.

Each cron job has a cursor (next_fire_at). For jobs in partitions we own, we compute the
firings up to now + horizon in the job's time zone (DST rules in core/schedule.py), apply
the misfire policy to firings that are already late, insert runs (UNIQUE(job_id,
scheduled_at) makes this idempotent), and advance the cursor. Paused tenants are not
materialized; on resume the backlog goes through the misfire policy.
"""
import asyncio
import logging
from datetime import timedelta

from app import repository as repo
from app.core.misfire import plan_materialization
from app.core.partitioning import partition_for
from app.core.schedule import fire_times_between, next_fire_times
from app.log_config import log_event
from app.metrics import MISFIRE_SKIPPED

log = logging.getLogger("materializer")


class Materializer:
    def __init__(self, pool, settings, membership):
        self.pool = pool
        self.s = settings
        self.membership = membership

    async def tick(self) -> int:
        owned = self.membership.owned
        if not owned:
            return 0
        created = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                jobs = await repo.lock_due_cron_jobs(conn, owned, self.s.num_partitions,
                                                     self.s.materialize_horizon_s)
                if not jobs:
                    return 0
                now = await repo.db_now(conn)
                end = now + timedelta(seconds=self.s.materialize_horizon_s)
                for j in jobs:
                    times = fire_times_between(j["cron"], j["timezone"], j["next_fire_at"], end)
                    keep, skipped = plan_materialization(j["misfire_policy"], times, now,
                                                         self.s.misfire_grace_s)
                    for t in keep:
                        rid = await repo.insert_run(
                            conn, job_id=j["id"], tenant_id=j["tenant_id"],
                            partition=partition_for(j["id"], self.s.num_partitions),
                            scheduled_at=t, priority=j["priority"],
                            misfire_policy=j["misfire_policy"])
                        created += rid is not None
                    if skipped:
                        MISFIRE_SKIPPED.inc(skipped)
                        log_event(log, "misfire_skipped_on_materialize", job_id=j["id"],
                                  policy=j["misfire_policy"], skipped=skipped)
                    nxt = next_fire_times(j["cron"], j["timezone"], end, 1)
                    await repo.advance_cursor(conn, j["id"], nxt[0])
        return created

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                log.exception("materializer_tick_failed")
            await asyncio.sleep(self.s.materialize_interval_s)
