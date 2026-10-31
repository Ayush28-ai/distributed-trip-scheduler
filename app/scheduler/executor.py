"""Runs claimed jobs and writes results back, fenced.

- Each run executes as an asyncio task, bounded by MAX_INFLIGHT and the job's max runtime.
- One loop renews the leases of all in-flight runs in a single UPDATE, matched on
  (run_id, fencing_token). A run that is not renewed has been taken over.
- Success: commit_if_token marks SUCCEEDED and records the effect in one transaction.
  If our token is stale the update matches 0 rows and nothing is written.
- Failure/timeout: retry with exponential backoff + full jitter, or DEAD + dead letter
  after max_attempts. Also fenced.
"""
import asyncio
import logging

from app import repository as repo
from app.core.backoff import backoff_delay
from app.handlers.base import RunContext
from app.log_config import log_event
from app.metrics import FAILED, INFLIGHT, LEASE_LOST, STALE_REJECTED, SUCCEEDED

log = logging.getLogger("executor")


class Executor:
    def __init__(self, pool, settings, handlers):
        self.pool = pool
        self.s = settings
        self.handlers = handlers
        self.inflight: dict[int, int] = {}  # run_id -> fencing_token
        self._tasks: set[asyncio.Task] = set()

    def capacity(self) -> int:
        return self.s.max_inflight - len(self.inflight)

    def submit(self, run) -> None:
        self.inflight[run["id"]] = run["fencing_token"]
        INFLIGHT.set(len(self.inflight))
        task = asyncio.create_task(self._execute(run))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _execute(self, run) -> None:
        ctx = RunContext(run_id=run["id"], job_id=run["job_id"], tenant_id=run["tenant_id"],
                         job_type=run["job_type"], scheduled_at=run["scheduled_at"],
                         attempt=run["attempt"], fencing_token=run["fencing_token"],
                         node_id=self.s.node_id, payload=run["payload"] or {})
        try:
            handler = self.handlers.get(ctx.job_type)
            if handler is None:
                raise RuntimeError(f"unknown job_type {ctx.job_type}")
            try:
                await asyncio.wait_for(handler.execute(ctx), timeout=run["max_runtime_s"])
            except asyncio.TimeoutError:
                raise RuntimeError(f"exceeded max runtime {run['max_runtime_s']}s")
            except Exception as e:
                await self._fail(run, ctx, e)
                return
            ok = await repo.commit_if_token(self.pool, ctx.run_id, ctx.fencing_token,
                                            ctx.idempotency_key, ctx.tenant_id, ctx.job_type)
            if ok:
                SUCCEEDED.inc()
            else:
                STALE_REJECTED.inc()
                log_event(log, "stale_commit_rejected", level=logging.WARNING, run_id=ctx.run_id,
                          token=ctx.fencing_token, node=self.s.node_id)
        except Exception as e:
            await self._fail(run, ctx, e)
        finally:
            self.inflight.pop(ctx.run_id, None)
            INFLIGHT.set(len(self.inflight))

    async def _fail(self, run, ctx: RunContext, err: Exception) -> None:
        dead = ctx.attempt >= run["max_attempts"]
        delay = None if dead else backoff_delay(ctx.attempt, self.s.backoff_base_s, self.s.backoff_cap_s)
        try:
            ok = await repo.fail_if_token(self.pool, ctx.run_id, ctx.fencing_token,
                                          f"{type(err).__name__}: {err}", delay)
        except Exception:
            log.exception("fail_write_error")
            return
        if not ok:
            STALE_REJECTED.inc()
            log_event(log, "stale_commit_rejected", level=logging.WARNING, run_id=ctx.run_id,
                      token=ctx.fencing_token, node=self.s.node_id)
            return
        FAILED.labels("dead" if dead else "retry").inc()
        log_event(log, "run_dead_lettered" if dead else "run_retry_scheduled",
                  run_id=ctx.run_id, attempt=ctx.attempt,
                  retry_in_s=None if dead else round(delay, 2), error=str(err)[:200])

    async def renew_loop(self) -> None:
        while True:
            await asyncio.sleep(self.s.run_lease_s / 3)
            if not self.inflight:
                continue
            ids, tokens = zip(*self.inflight.items())
            try:
                renewed = await repo.renew_leases(self.pool, list(ids), list(tokens), self.s.run_lease_s)
            except Exception:
                log.exception("lease_renew_failed")
                continue
            lost = [i for i in ids if i not in renewed and i in self.inflight]
            if lost:
                LEASE_LOST.inc(len(lost))
                # The handler keeps running; its final write will be rejected by the fencing token.
                log_event(log, "leases_lost", level=logging.WARNING, count=len(lost),
                          sample=lost[:5], node=self.s.node_id)

    async def close(self) -> None:
        for h in self.handlers.values():
            await h.close()
