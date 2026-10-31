"""Local job types. Their effect is the row the executor writes to `effects`
in the same transaction as the fenced commit."""
import asyncio
import logging
import random

from app.handlers.base import JobHandler, RunContext
from app.log_config import log_event

log = logging.getLogger("handlers")


class _LoggingHandler(JobHandler):
    async def execute(self, ctx: RunContext) -> None:
        await asyncio.sleep(int(ctx.payload.get("simulate_ms", 0)) / 1000)
        log_event(log, f"{self.job_type}_done", run_id=ctx.run_id, tenant=ctx.tenant_id)


class RosterHandler(_LoggingHandler):
    job_type = "roster"        # nightly roster generation


class BillingHandler(_LoggingHandler):
    job_type = "billing"       # daily billing close


class NoShowHandler(_LoggingHandler):
    job_type = "no_show"       # auto-cancel trip when employee does not show


class NoopHandler(JobHandler):
    job_type = "noop"          # used by the benchmark

    async def execute(self, ctx: RunContext) -> None:
        return None


class FlakyHandler(JobHandler):
    """Fails with probability payload.fail_rate (default 0.5). fail_rate=1 -> always dead-letters."""
    job_type = "flaky"

    async def execute(self, ctx: RunContext) -> None:
        if random.random() < float(ctx.payload.get("fail_rate", 0.5)):
            raise RuntimeError("simulated downstream failure")
