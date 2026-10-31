"""The SIGSTOP scenario in a test: node A claims, freezes past its lease, node B takes
over and commits; when A wakes up, every write it tries is rejected."""
import asyncio

from app import repository as repo
from app.db import create_pool
from app.handlers.base import idempotency_key
from tests.conftest import reset, seed_runs

ALL = list(range(32))


async def _expire_all_leases(pool) -> None:
    await pool.execute("UPDATE job_runs SET lease_until = now() - interval '1 second' WHERE status = 'RUNNING'")


async def _stale_commit_rejected(url: str) -> None:
    pool = await create_pool(url)
    try:
        await reset(pool)
        await seed_runs(pool, "acme", 1)
        [a] = await repo.claim(pool, ALL, "acme", 10, "nodeA", 10)

        await _expire_all_leases(pool)                         # A is frozen; lease runs out
        assert await repo.reset_expired(pool, ALL) == 1
        [b] = await repo.claim(pool, ALL, "acme", 10, "nodeB", 10)
        assert b["id"] == a["id"] and b["fencing_token"] > a["fencing_token"]
        assert b["attempt"] == 2 and not b["first_claim"]

        key = idempotency_key(b["tenant_id"], b["job_id"], b["scheduled_at"])
        assert await repo.commit_if_token(pool, b["id"], b["fencing_token"], key, "acme", "noop")

        # A wakes up: renew, commit and fail are all rejected
        assert await repo.renew_leases(pool, [a["id"]], [a["fencing_token"]], 10) == set()
        assert not await repo.commit_if_token(pool, a["id"], a["fencing_token"], key, "acme", "noop")
        assert not await repo.fail_if_token(pool, a["id"], a["fencing_token"], "late", 1.0)

        run = await pool.fetchrow("SELECT status, fencing_token, owner_node FROM job_runs WHERE id = $1", a["id"])
        assert run["status"] == "SUCCEEDED" and run["fencing_token"] == b["fencing_token"]
        effect = await pool.fetch("SELECT fencing_token FROM effects")
        assert len(effect) == 1 and effect[0]["fencing_token"] == b["fencing_token"]
    finally:
        await pool.close()


async def _stale_write_rejected_before_takeover(url: str) -> None:
    """After the reset but before B claims, A's write is also rejected (status changed)."""
    pool = await create_pool(url)
    try:
        await reset(pool)
        await seed_runs(pool, "acme", 1)
        [a] = await repo.claim(pool, ALL, "acme", 10, "nodeA", 10)
        await _expire_all_leases(pool)
        await repo.reset_expired(pool, ALL)
        key = idempotency_key(a["tenant_id"], a["job_id"], a["scheduled_at"])
        assert not await repo.commit_if_token(pool, a["id"], a["fencing_token"], key, "acme", "noop")
        assert await pool.fetchval("SELECT count(*) FROM effects") == 0
    finally:
        await pool.close()


async def _retries_then_dead_letter_and_replay(url: str) -> None:
    pool = await create_pool(url)
    try:
        await reset(pool)
        await seed_runs(pool, "acme", 1)
        await pool.execute("UPDATE jobs SET max_attempts = 2")
        [r1] = await repo.claim(pool, ALL, "acme", 1, "n", 10)
        assert await repo.fail_if_token(pool, r1["id"], r1["fencing_token"], "boom", 0.0)   # retry
        [r2] = await repo.claim(pool, ALL, "acme", 1, "n", 10)
        assert r2["attempt"] == 2
        assert await repo.fail_if_token(pool, r2["id"], r2["fencing_token"], "boom", None)  # dead
        [dl] = await repo.list_dead_letters(pool, "acme")
        assert dl["attempts"] == 2
        assert await repo.replay_dead_letter(pool, dl["id"]) == r2["id"]
        assert await repo.replay_dead_letter(pool, dl["id"]) is None                       # only once
        [r3] = await repo.claim(pool, ALL, "acme", 1, "n", 10)
        assert r3["id"] == r2["id"] and r3["attempt"] == 1
    finally:
        await pool.close()


def test_stale_owner_cannot_commit_after_takeover(test_db_url):
    asyncio.run(_stale_commit_rejected(test_db_url))


def test_stale_owner_cannot_commit_after_lease_reset(test_db_url):
    asyncio.run(_stale_write_rejected_before_takeover(test_db_url))


def test_retry_dead_letter_and_replay(test_db_url):
    asyncio.run(_retries_then_dead_letter_and_replay(test_db_url))
