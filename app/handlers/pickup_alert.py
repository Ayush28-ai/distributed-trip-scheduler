"""Pickup alert: the side effect lives in the Spring Boot notification-service,
which de-duplicates on the Idempotency-Key header (unique constraint)."""
import asyncio

import httpx

from app.handlers.base import JobHandler, RunContext


class PickupAlertHandler(JobHandler):
    job_type = "pickup_alert"

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(5.0),
                limits=httpx.Limits(max_connections=200, max_keepalive_connections=100))
        return self._client

    async def execute(self, ctx: RunContext) -> None:
        # Optional simulated work, used by the kill -9 / SIGSTOP tests to keep runs in flight.
        simulate_ms = int(ctx.payload.get("simulate_ms", 0))
        if simulate_ms:
            await asyncio.sleep(simulate_ms / 1000)
        resp = await self._http().post(
            f"{self.base_url}/notifications",
            json={
                "tenantId": ctx.tenant_id,
                "tripId": str(ctx.payload.get("trip_id", "")),
                "message": ctx.payload.get("message", "Your cab is assigned"),
            },
            headers={
                "Idempotency-Key": ctx.idempotency_key,
                "Fencing-Token": str(ctx.fencing_token),
                "Node-Id": ctx.node_id,
            },
        )
        # 201 = sent now, 200 = duplicate suppressed (already sent). Both are success.
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"notification-service returned {resp.status_code}")

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
