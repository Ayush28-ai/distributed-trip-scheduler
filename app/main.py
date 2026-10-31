"""FastAPI app: uvicorn app.main:app --host 0.0.0.0 --port 8000"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel

from app import repository as repo
from app.api import jobs, ops, tenants
from app.auth import authenticate, issue_token
from app.config import settings
from app.db import create_pool
from app.log_config import setup_logging
from app.metrics import cluster_metrics_text


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(settings.log_level)
    for _ in range(30):  # wait for Postgres
        try:
            app.state.pool = await create_pool(settings.database_url, min_size=2, max_size=10)
            break
        except Exception:
            await asyncio.sleep(2)
    else:
        raise RuntimeError("database unavailable")
    yield
    await app.state.pool.close()


app = FastAPI(title="Cloudstream distributed trip scheduler", version="1.0.0", lifespan=lifespan)
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
async def health(request: Request):
    await request.app.state.pool.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics", tags=["ops"])
async def metrics(request: Request):
    """Cluster-wide Prometheus metrics computed from the DB (no auth, for scrapers)."""
    stats = await repo.stats(request.app.state.pool)
    return Response(cluster_metrics_text(stats), media_type="text/plain; version=0.0.4")
