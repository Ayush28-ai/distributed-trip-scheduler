"""Job handler contract. Each job_type is one subclass (polymorphism)."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


def idempotency_key(tenant_id: str, job_id: int, scheduled_at: datetime) -> str:
    """Identifies ONE firing. Retries and takeovers reuse it, so the effect happens once."""
    return f"{tenant_id}:{job_id}:{scheduled_at.isoformat()}"


@dataclass
class RunContext:
    run_id: int
    job_id: int
    tenant_id: str
    job_type: str
    scheduled_at: datetime
    attempt: int
    fencing_token: int
    node_id: str
    payload: dict = field(default_factory=dict)

    @property
    def idempotency_key(self) -> str:
        return idempotency_key(self.tenant_id, self.job_id, self.scheduled_at)


class JobHandler(ABC):
    job_type: str = ""

    @abstractmethod
    async def execute(self, ctx: RunContext) -> None:
        """Do the work. Raise to signal failure (the run is retried with backoff).
        Must be safe to call more than once for the same ctx.idempotency_key."""

    async def close(self) -> None:  # optional cleanup
        return None
