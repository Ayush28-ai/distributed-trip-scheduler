from app import repository as repo
from app.errors import NotFoundError


class TenantService:
    def __init__(self, pool):
        self.pool = pool

    async def upsert(self, tenant_id: str, timezone: str, max_concurrency: int) -> dict:
        return dict(await repo.upsert_tenant(self.pool, tenant_id, timezone, max_concurrency))

    async def list(self, tenant_id: str | None) -> list[dict]:
        return [dict(r) for r in await repo.list_tenants(self.pool, tenant_id)]

    async def set_paused(self, tenant_id: str, paused: bool) -> dict:
        """Pause stops dispatching within TENANT_CACHE_TTL_S; queued runs stay queued and
        go through their misfire policy on resume."""
        if not await repo.set_tenant_paused(self.pool, tenant_id, paused):
            raise NotFoundError("tenant not found")
        return {"tenant_id": tenant_id, "paused": paused}
