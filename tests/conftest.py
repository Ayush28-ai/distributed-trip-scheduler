"""DB tests use a separate database (scheduler_test) so running nodes never touch test data.
Start the stack first (docker compose up) or any Postgres on localhost:5432.
Override with TEST_DATABASE_URL."""
import asyncio
import os
from pathlib import Path

import asyncpg
import pytest

ADMIN_URL = os.getenv("TEST_ADMIN_URL", "postgresql://scheduler:scheduler@localhost:5432/scheduler")
TEST_URL = os.getenv("TEST_DATABASE_URL", "postgresql://scheduler:scheduler@localhost:5432/scheduler_test")
SCHEMA = (Path(__file__).resolve().parents[1] / "migrations" / "schema.sql").read_text()


async def _prepare() -> None:
    admin = await asyncpg.connect(ADMIN_URL)
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = 'scheduler_test'"):
            await admin.execute("CREATE DATABASE scheduler_test")
    finally:
        await admin.close()
    conn = await asyncpg.connect(TEST_URL)
    try:
        await conn.execute(SCHEMA)
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def test_db_url() -> str:
    try:
        asyncio.run(_prepare())
    except Exception as e:  # no Postgres reachable
        pytest.skip(f"Postgres not reachable for DB tests: {e}")
    return TEST_URL


async def reset(pool) -> None:
    await pool.execute("""TRUNCATE effects, dead_letters, notifications, job_runs, jobs, tenants, nodes
                          RESTART IDENTITY CASCADE""")
    await pool.execute("UPDATE partitions SET owner_node = NULL, lease_until = NULL")


async def seed_runs(pool, tenant: str, count: int, *, job_type: str = "noop", cap: int = 10_000,
                    due_in_s: float = 0.0, priority: int = 5) -> None:
    """Create `count` one-off jobs (and their runs) for a tenant, due now + due_in_s."""
    await pool.execute(
        """INSERT INTO tenants (id, max_concurrency) VALUES ($1, $2)
           ON CONFLICT (id) DO UPDATE SET max_concurrency = $2""", tenant, cap)
    await pool.execute(
        """WITH j AS (
               INSERT INTO jobs (tenant_id, name, job_type, run_at, priority)
               SELECT $1, 'job-' || g, $3, now() + make_interval(secs => $4), $5
                 FROM generate_series(1, $2) g
               RETURNING id, tenant_id, run_at, priority, misfire_policy)
           INSERT INTO job_runs (job_id, tenant_id, partition, scheduled_at, next_attempt_at,
                                 priority, misfire_policy)
           SELECT id, tenant_id, id % 32, run_at, run_at, priority, misfire_policy FROM j""",
        tenant, count, job_type, float(due_in_s), priority)
