from fastapi import APIRouter, Depends, HTTPException, Request

from app import repository as repo
from app.auth import User, check_tenant, current_user, tenant_scope

router = APIRouter(tags=["operations"])


@router.get("/runs")
async def list_runs(request: Request, tenant_id: str | None = None, status: str | None = None,
                    limit: int = 200, user: User = Depends(current_user)):
    rows = await repo.list_runs(request.app.state.pool, tenant_scope(user, tenant_id), status,
                                min(limit, 1000))
    return [dict(r) for r in rows]


@router.get("/dead-letters")
async def list_dead_letters(request: Request, tenant_id: str | None = None,
                            include_replayed: bool = False, user: User = Depends(current_user)):
    rows = await repo.list_dead_letters(request.app.state.pool, tenant_scope(user, tenant_id),
                                        include_replayed)
    return [dict(r) for r in rows]


@router.post("/dead-letters/{dl_id}/replay")
async def replay(dl_id: int, request: Request, user: User = Depends(current_user)):
    pool = request.app.state.pool
    dl = await repo.get_dead_letter(pool, dl_id)
    if dl is None:
        raise HTTPException(404, "dead letter not found")
    check_tenant(user, dl["tenant_id"])
    run_id = await repo.replay_dead_letter(pool, dl_id)
    if run_id is None:
        raise HTTPException(409, "already replayed")
    return {"dead_letter_id": dl_id, "run_id": run_id, "status": "SCHEDULED"}


@router.get("/stats")
async def stats(request: Request, tenant_id: str | None = None, user: User = Depends(current_user)):
    return await repo.stats(request.app.state.pool, tenant_scope(user, tenant_id))


@router.get("/cluster")
async def cluster(request: Request, user: User = Depends(current_user)):
    rows = await repo.cluster_view(request.app.state.pool)
    return [{"node_id": r["node_id"], "heartbeat_age_s": round(r["heartbeat_age_s"], 1),
             "alive": r["heartbeat_age_s"] < 6, "partitions": list(r["partitions"])} for r in rows]
