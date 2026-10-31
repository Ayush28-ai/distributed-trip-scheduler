"""Job controller: HTTP + auth only; rules live in JobService."""
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, model_validator

from app.api.deps import job_service
from app.auth import User, check_tenant, current_user, require_writer, tenant_scope
from app.core.misfire import POLICIES
from app.core.schedule import is_valid_cron, is_valid_timezone
from app.handlers import JOB_TYPES
from app.services import JobService

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


async def _owned_job(job_id: int, user: User, svc: JobService) -> dict:
    job = await svc.get(job_id)
    check_tenant(user, job["tenant_id"])
    return job


@router.post("", status_code=201)
async def create_job(body: JobIn, user: User = Depends(require_writer),
                     svc: JobService = Depends(job_service)):
    check_tenant(user, body.tenant_id)
    return await svc.create(body)


@router.get("")
async def list_jobs(tenant_id: str | None = None, limit: int = 100,
                    user: User = Depends(current_user), svc: JobService = Depends(job_service)):
    return await svc.list(tenant_scope(user, tenant_id), limit)


@router.get("/{job_id}")
async def get_job(job_id: int, user: User = Depends(current_user),
                  svc: JobService = Depends(job_service)):
    return await _owned_job(job_id, user, svc)


@router.get("/{job_id}/next-fire-times")
async def get_next_fire_times(job_id: int, n: int = 5, user: User = Depends(current_user),
                              svc: JobService = Depends(job_service)):
    return await svc.next_fire_times(await _owned_job(job_id, user, svc), n)


@router.post("/{job_id}/trigger", status_code=201)
async def trigger_job(job_id: int, user: User = Depends(require_writer),
                      svc: JobService = Depends(job_service)):
    return await svc.trigger(await _owned_job(job_id, user, svc))
