"""All settings come from environment variables, with defaults that work locally."""
import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://scheduler:scheduler@localhost:5432/scheduler"
    )
    node_id: str = os.getenv("NODE_ID", "node-local")

    # Partitioning: fixed 32 partitions (schema seeds 0..31), partition = job_id % 32
    num_partitions: int = 32
    partition_lease_s: float = _float("PARTITION_LEASE_S", 10)
    heartbeat_s: float = _float("HEARTBEAT_S", 2)
    node_dead_after_s: float = _float("NODE_DEAD_AFTER_S", 6)

    # Run leases + execution
    run_lease_s: float = _float("RUN_LEASE_S", 10)
    max_inflight: int = _int("MAX_INFLIGHT", 200)          # per node
    poll_interval_s: float = _float("POLL_INTERVAL_S", 0.1)
    claim_batch: int = _int("CLAIM_BATCH", 100)

    # Retries
    backoff_base_s: float = _float("BACKOFF_BASE_S", 1)
    backoff_cap_s: float = _float("BACKOFF_CAP_S", 60)

    # Misfire + materialization
    misfire_grace_s: float = _float("MISFIRE_GRACE_S", 60)
    materialize_horizon_s: float = _float("MATERIALIZE_HORIZON_S", 60)
    materialize_interval_s: float = _float("MATERIALIZE_INTERVAL_S", 5)

    # Tenant cache TTL (pause/resume takes effect within this many seconds)
    tenant_cache_ttl_s: float = _float("TENANT_CACHE_TTL_S", 2)

    # Spring notification-service
    notification_url: str = os.getenv("NOTIFICATION_URL", "http://localhost:8080")

    # Auth
    jwt_secret: str = os.getenv("JWT_SECRET", "dev-secret-change-me")
    jwt_ttl_s: int = _int("JWT_TTL_S", 8 * 3600)
    admin_user: str = os.getenv("ADMIN_USER", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "admin123")
    # "user:password:tenant,user:password:tenant"
    operators: str = os.getenv("OPERATORS", "acme_ops:acme123:acme,globex_ops:globex123:globex")

    metrics_port: int = _int("METRICS_PORT", 9100)
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
