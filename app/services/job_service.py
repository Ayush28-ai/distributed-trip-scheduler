from zoneinfo import ZoneInfo

from app import repository as repo
from app.core.partitioning import partition_for
from app.core.schedule import next_fire_times
from app.errors import NotFoundError


class JobService:
    def __init__(self, pool, num_partitions: int):
        self.pool = pool
        self.num_partitions = num_partitions

    async def create(self, spec) -> dict:
        """spec: validated JobIn. One-off jobs get their run immediately; cron jobs get a
        cursor (next_fire_at) that the materializer turns into runs."""
        tenant = await repo.get_tenant(self.pool, spec.tenant_id)
        if tenant is None:
            raise NotFoundError("tenant not found")
        tz = spec.timezone or tenant["timezone"]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                now = await repo.db_now(conn)
                next_fire = next_fire_times(spec.cron, tz, now, 1)[0] if spec.cron else None
                job = await repo.insert_job(
                    conn, tenant_id=spec.tenant_id, name=spec.name, job_type=spec.job_type,
                    cron=spec.cron, run_at=spec.run_at, timezone=tz, priority=spec.priority,
                    max_runtime_s=spec.max_runtime_s, max_attempts=spec.max_attempts,
                    misfire_policy=spec.misfire_policy, payload=spec.payload, next_fire_at=next_fire)
                if spec.run_at is not None:
                    await repo.insert_run(
                        conn, job_id=job["id"], tenant_id=spec.tenant_id,
                        partition=partition_for(job["id"], self.num_partitions),
                        scheduled_at=spec.run_at, priority=spec.priority,
                        misfire_policy=spec.misfire_policy)
        return dict(job)

    async def get(self, job_id: int) -> dict:
        job = await repo.get_job(self.pool, job_id)
        if job is None:
            raise NotFoundError("job not found")
        return dict(job)

    async def list(self, tenant_id: str | None, limit: int) -> list[dict]:
        return [dict(r) for r in await repo.list_jobs(self.pool, tenant_id, min(max(limit, 1), 1000))]

    async def next_fire_times(self, job: dict, n: int) -> dict:
        if job["cron"] is None:
            return {"job_id": job["id"], "timezone": job["timezone"], "next_utc": [job["run_at"]]}
        now = await repo.db_now(self.pool)
        times = next_fire_times(job["cron"], job["timezone"], now, min(max(n, 1), 50))
        tz = ZoneInfo(job["timezone"])
        return {"job_id": job["id"], "cron": job["cron"], "timezone": job["timezone"],
                "next_utc": times, "next_local": [t.astimezone(tz).isoformat() for t in times]}

    async def trigger(self, job: dict) -> dict:
        run_id = await repo.trigger_now(self.pool, job, partition_for(job["id"], self.num_partitions))
        return {"job_id": job["id"], "run_id": run_id}
