"""Dependency providers: controllers get services, never the pool or SQL."""
from fastapi import Request

from app.services import JobService, OpsService, TenantService


def job_service(request: Request) -> JobService:
    return request.app.state.job_service


def tenant_service(request: Request) -> TenantService:
    return request.app.state.tenant_service


def ops_service(request: Request) -> OpsService:
    return request.app.state.ops_service
