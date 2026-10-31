"""Tenant controller: HTTP + auth only; rules live in TenantService."""
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, field_validator

from app.api.deps import tenant_service
from app.auth import User, check_tenant, current_user, require_admin, require_writer, tenant_scope
from app.core.schedule import is_valid_timezone
from app.services import TenantService

router = APIRouter(prefix="/tenants", tags=["tenants"])


class TenantIn(BaseModel):
    id: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9_\-]+$")
    timezone: str = "UTC"
    max_concurrency: int = Field(50, ge=1, le=10_000)

    @field_validator("timezone")
    @classmethod
    def _tz(cls, v: str) -> str:
        if not is_valid_timezone(v):
            raise ValueError("unknown time zone")
        return v


@router.post("", status_code=201)
async def upsert_tenant(body: TenantIn, _: User = Depends(require_admin),
                        svc: TenantService = Depends(tenant_service)):
    return await svc.upsert(body.id, body.timezone, body.max_concurrency)


@router.get("")
async def list_tenants(user: User = Depends(current_user), svc: TenantService = Depends(tenant_service)):
    return await svc.list(tenant_scope(user, None))


@router.post("/{tenant_id}/pause")
async def pause(tenant_id: str, user: User = Depends(require_writer),
                svc: TenantService = Depends(tenant_service)):
    check_tenant(user, tenant_id)
    return await svc.set_paused(tenant_id, True)


@router.post("/{tenant_id}/resume")
async def resume(tenant_id: str, user: User = Depends(require_writer),
                 svc: TenantService = Depends(tenant_service)):
    check_tenant(user, tenant_id)
    return await svc.set_paused(tenant_id, False)
