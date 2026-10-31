"""Operations controller: runs, dead letters, stats, cluster view."""
from fastapi import APIRouter, Depends

from app.api.deps import ops_service
from app.auth import User, check_tenant, current_user, require_admin, require_writer, tenant_scope
from app.services import OpsService

router = APIRouter(tags=["operations"])


@router.get("/runs")
async def list_runs(tenant_id: str | None = None, status: str | None = None, limit: int = 200,
                    user: User = Depends(current_user), svc: OpsService = Depends(ops_service)):
    return await svc.runs(tenant_scope(user, tenant_id), status, limit)


@router.get("/dead-letters")
async def list_dead_letters(tenant_id: str | None = None, include_replayed: bool = False,
                            user: User = Depends(current_user), svc: OpsService = Depends(ops_service)):
    return await svc.dead_letters(tenant_scope(user, tenant_id), include_replayed)


@router.post("/dead-letters/{dl_id}/replay")
async def replay(dl_id: int, user: User = Depends(require_writer),
                 svc: OpsService = Depends(ops_service)):
    check_tenant(user, (await svc.dead_letter(dl_id))["tenant_id"])
    return await svc.replay(dl_id)


@router.get("/stats")
async def stats(tenant_id: str | None = None, user: User = Depends(current_user),
                svc: OpsService = Depends(ops_service)):
    return await svc.stats(tenant_scope(user, tenant_id))


@router.get("/cluster")
async def cluster(_: User = Depends(require_admin), svc: OpsService = Depends(ops_service)):
    return await svc.cluster()
