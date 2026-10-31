from app import repository as repo
from app.errors import ConflictError, DomainError, NotFoundError

RUN_STATUSES = {"SCHEDULED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "DEAD", "SKIPPED"}


class OpsService:
    def __init__(self, pool, node_dead_after_s: float):
        self.pool = pool
        self.node_dead_after_s = node_dead_after_s

    async def runs(self, tenant_id: str | None, status: str | None, limit: int) -> list[dict]:
        if status is not None and status not in RUN_STATUSES:
            raise DomainError(f"status must be one of {sorted(RUN_STATUSES)}")
        return [dict(r) for r in await repo.list_runs(self.pool, tenant_id, status, min(max(limit, 1), 1000))]

    async def dead_letters(self, tenant_id: str | None, include_replayed: bool) -> list[dict]:
        return [dict(r) for r in await repo.list_dead_letters(self.pool, tenant_id, include_replayed)]

    async def dead_letter(self, dl_id: int) -> dict:
        dl = await repo.get_dead_letter(self.pool, dl_id)
        if dl is None:
            raise NotFoundError("dead letter not found")
        return dict(dl)

    async def replay(self, dl_id: int) -> dict:
        """Re-queue a dead run. Same run, same idempotency key: a replay cannot duplicate an effect."""
        run_id = await repo.replay_dead_letter(self.pool, dl_id)
        if run_id is None:
            raise ConflictError("dead letter already replayed")
        return {"dead_letter_id": dl_id, "run_id": run_id, "status": "SCHEDULED"}

    async def stats(self, tenant_id: str | None) -> dict:
        return await repo.stats(self.pool, tenant_id)

    async def cluster(self) -> list[dict]:
        rows = await repo.cluster_view(self.pool)
        return [{"node_id": r["node_id"], "heartbeat_age_s": round(r["heartbeat_age_s"], 1),
                 "alive": r["heartbeat_age_s"] < self.node_dead_after_s,
                 "partitions": list(r["partitions"])} for r in rows]
