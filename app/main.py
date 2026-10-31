"""FastAPI app: uvicorn app.main:app --host 0.0.0.0 --port 8000
Layers: controllers (app/api) -> services (app/services) -> repository (app/repository.py)."""
import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from app.api import jobs, ops, tenants
from app.api.deps import ops_service
from app.auth import authenticate, issue_token
from app.config import settings
from app.db import create_pool
from app.errors import install_error_handlers
from app.log_config import setup_logging
from app.metrics import cluster_metrics_text
from app.services import JobService, OpsService, TenantService


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(settings.log_level)
    for _ in range(30):  # wait for Postgres
        try:
            pool = await create_pool(settings.database_url, min_size=2, max_size=10)
            break
        except Exception:
            await asyncio.sleep(2)
    else:
        raise RuntimeError("database unavailable")
    app.state.pool = pool
    app.state.job_service = JobService(pool, settings.num_partitions)
    app.state.tenant_service = TenantService(pool)
    app.state.ops_service = OpsService(pool, settings.node_dead_after_s)
    yield
    await pool.close()


app = FastAPI(title="Cloudstream distributed trip scheduler", version="1.1.0", lifespan=lifespan)
install_error_handlers(app)
app.include_router(tenants.router)
app.include_router(jobs.router)
app.include_router(ops.router)


class LoginIn(BaseModel):
    username: str
    password: str


@app.post("/auth/token", tags=["auth"])
async def login(body: LoginIn):
    user = authenticate(body.username, body.password)
    if user is None:
        raise HTTPException(401, "bad credentials")
    return {"access_token": issue_token(user), "token_type": "bearer",
            "role": user.role, "tenant": user.tenant}


@app.get("/health", tags=["ops"])
async def health(svc: OpsService = Depends(ops_service)):
    await svc.pool.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics", tags=["ops"])
async def metrics(svc: OpsService = Depends(ops_service)):
    """Cluster-wide Prometheus metrics computed from the DB (no auth, for scrapers)."""
    return Response(cluster_metrics_text(await svc.stats(None)),
                    media_type="text/plain; version=0.0.4")
