from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from app import repository as repo
from app.auth import User, check_tenant, current_user, tenant_scope
from app.config import settings
from app.core.misfire import POLICIES
from app.core.partitioning import partition_for
from app.core.schedule import is_valid_cron, is_valid_timezone, next_fire_times
from app.handlers import JOB_TYPES

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobIn(BaseModel):
    tenant_id: str
    name: str = Field(min_length=1, max_length=200)
    job_type: str
    cron: str | None = None                 # e.g. "0 22 * * *" (10 PM in `timezone`)
    run_at: datetime | None = None          # one-off, must include an offset
    timezone: str | None = None             # default: tenant's time zone
    priority: int = Field(5, ge=0, le=10)
    max_runtime_s: int = Field(30, ge=1, le=3600)
    max_attempts: int = Field(5, ge=1, le=20)
    misfire_policy: str = "FIRE_NOW"
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self):
        if (self.cron is None) == (self.run_at is None):
            raise ValueError("give exactly one of cron or run_at")
        if self.cron is not None and not is_valid_cron(self.cron):
            raise ValueError("invalid cron expression")
        if self.run_at is not None and self.run_at.tzinfo is None:
            raise ValueError("run_at must include a UTC offset, e.g. 2026-11-01T08:30:00+05:30")
        if self.timezone is not None and not is_valid_timezone(self.timezone):
            raise ValueError("unknown time zone")
        if self.misfire_policy not in POLICIES:
            raise ValueError(f"misfire_policy must be one of {POLICIES}")
        if self.job_type not in JOB_TYPES:
            raise ValueError(f"job_type must be one of {JOB_TYPES}")
        return self


def _pool(request: Request):
    return request.app.state.pool


@router.post("", status_code=201)
async def create_job(body: JobIn, request: Request, user: User = Depends(current_user)):
    check_tenant(user, body.tenant_id)
    pool = _pool(request)
    tenant = await repo.get_tenant(pool, body.tenant_id)
    if tenant is None:
        raise HTTPException(404, "tenant not found")
    tz = body.timezone or tenant["timezone"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            now = await repo.db_now(conn)
            next_fire = next_fire_times(body.cron, tz, now, 1)[0] if body.cron else None
            job = await repo.insert_job(
                conn, tenant_id=body.tenant_id, name=body.name, job_type=body.job_type,
                cron=body.cron, run_at=body.run_at, timezone=tz, priority=body.priority,
                max_runtime_s=body.max_runtime_s, max_attempts=body.max_attempts,
                misfire_policy=body.misfire_policy, payload=body.payload, next_fire_at=next_fire)
            if body.run_at is not None:
                await repo.insert_run(
                    conn, job_id=job["id"], tenant_id=body.tenant_id,
                    partition=partition_for(job["id"], settings.num_partitions),
                    scheduled_at=body.run_at, priority=body.priority,
                    misfire_policy=body.misfire_policy)
    return dict(job)


@router.get("")
async def list_jobs(request: Request, tenant_id: str | None = None, limit: int = 100,
                    user: User = Depends(current_user)):
    return [dict(r) for r in await repo.list_jobs(_pool(request), tenant_scope(user, tenant_id), limit)]


async def _job_for(request: Request, job_id: int, user: User):
    job = await repo.get_job(_pool(request), job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    check_tenant(user, job["tenant_id"])
    return job


@router.get("/{job_id}")
async def get_job(job_id: int, request: Request, user: User = Depends(current_user)):
    return dict(await _job_for(request, job_id, user))


@router.get("/{job_id}/next-fire-times")
async def get_next_fire_times(job_id: int, request: Request, n: int = 5,
                              user: User = Depends(current_user)):
    job = await _job_for(request, job_id, user)
    if job["cron"] is None:
        return {"job_id": job_id, "timezone": job["timezone"], "next": [job["run_at"]]}
    now = await repo.db_now(_pool(request))
    times = next_fire_times(job["cron"], job["timezone"], now, min(max(n, 1), 50))
    return {"job_id": job_id, "cron": job["cron"], "timezone": job["timezone"],
            "next_utc": times,
            "next_local": [t.astimezone(ZoneInfo(job["timezone"])).isoformat() for t in times]}


@router.post("/{job_id}/trigger", status_code=201)
async def trigger_job(job_id: int, request: Request, user: User = Depends(current_user)):
    job = await _job_for(request, job_id, user)
    run_id = await repo.trigger_now(_pool(request), job, partition_for(job_id, settings.num_partitions))
    return {"job_id": job_id, "run_id": run_id}
