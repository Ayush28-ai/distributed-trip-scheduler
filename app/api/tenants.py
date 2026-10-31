from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app import repository as repo
from app.auth import User, check_tenant, current_user, require_admin, tenant_scope
from app.core.schedule import is_valid_timezone

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
async def upsert_tenant(body: TenantIn, request: Request, _: User = Depends(require_admin)):
    return dict(await repo.upsert_tenant(request.app.state.pool, body.id, body.timezone,
                                         body.max_concurrency))


@router.get("")
async def list_tenants(request: Request, user: User = Depends(current_user)):
    return [dict(r) for r in await repo.list_tenants(request.app.state.pool, tenant_scope(user, None))]


async def _set_paused(request: Request, tenant_id: str, user: User, paused: bool):
    check_tenant(user, tenant_id)
    if not await repo.set_tenant_paused(request.app.state.pool, tenant_id, paused):
        raise HTTPException(404, "tenant not found")
    return {"tenant_id": tenant_id, "paused": paused}


@router.post("/{tenant_id}/pause")
async def pause(tenant_id: str, request: Request, user: User = Depends(current_user)):
    return await _set_paused(request, tenant_id, user, True)


@router.post("/{tenant_id}/resume")
async def resume(tenant_id: str, request: Request, user: User = Depends(current_user)):
    return await _set_paused(request, tenant_id, user, False)
