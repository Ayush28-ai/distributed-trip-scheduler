"""Roles, tenant isolation and non-leaking errors. Runs against the real app with fake
services, so no database is needed."""
from fastapi.testclient import TestClient

from app.auth import authenticate, issue_token
from app.main import app

client = TestClient(app, raise_server_exceptions=False)   # no `with`: lifespan (DB) not started

JOB = {"tenant_id": "acme", "name": "pickup", "job_type": "pickup_alert",
       "run_at": "2026-11-01T08:30:00+05:30", "payload": {"trip_id": "T1"}}


class FakeJobs:
    async def create(self, spec):
        return {"id": 1, "tenant_id": spec.tenant_id}

    async def list(self, tenant_id, limit):
        return [{"id": 1, "tenant_id": tenant_id}]

    async def get(self, job_id):
        return {"id": job_id, "tenant_id": "acme", "cron": None, "run_at": None, "timezone": "UTC"}

    async def trigger(self, job):
        raise RuntimeError("db exploded at 10.0.0.5 for phone 9876543210")


class FakeTenants:
    async def set_paused(self, tenant_id, paused):
        return {"tenant_id": tenant_id, "paused": paused}


app.state.job_service = FakeJobs()
app.state.tenant_service = FakeTenants()


def _auth(user: str, pwd: str) -> dict:
    return {"Authorization": f"Bearer {issue_token(authenticate(user, pwd))}"}


ADMIN = _auth("admin", "admin123")
ACME_OPS = _auth("acme_ops", "acme123")
GLOBEX_OPS = _auth("globex_ops", "globex123")
ACME_EMP = _auth("acme_emp", "emp123")


def test_no_token_is_401():
    r = client.get("/jobs")
    assert r.status_code == 401 and r.json()["error"] == "unauthorized"


def test_bad_password_rejected():
    assert authenticate("admin", "wrong") is None
    assert client.post("/auth/token", json={"username": "admin", "password": "x"}).status_code == 401


def test_operator_cannot_touch_other_tenant():
    assert client.post("/jobs", json=JOB, headers=GLOBEX_OPS).status_code == 403
    assert client.post("/tenants/acme/pause", headers=GLOBEX_OPS).status_code == 403
    assert client.get("/jobs?tenant_id=acme", headers=GLOBEX_OPS).status_code == 403
    assert client.get("/jobs/1", headers=GLOBEX_OPS).status_code == 403   # job belongs to acme


def test_operator_can_write_own_tenant():
    assert client.post("/jobs", json=JOB, headers=ACME_OPS).status_code == 201
    assert client.post("/tenants/acme/pause", headers=ACME_OPS).json()["paused"] is True


def test_employee_is_read_only_and_tenant_scoped():
    r = client.get("/jobs", headers=ACME_EMP)
    assert r.status_code == 200 and r.json()[0]["tenant_id"] == "acme"
    assert client.post("/jobs", json=JOB, headers=ACME_EMP).status_code == 403
    assert client.post("/tenants/acme/pause", headers=ACME_EMP).status_code == 403
    assert client.post("/jobs/1/trigger", headers=ACME_EMP).status_code == 403
    assert client.post("/dead-letters/1/replay", headers=ACME_EMP).status_code == 403


def test_only_admin_creates_tenants_and_sees_cluster():
    body = {"id": "newco", "timezone": "UTC"}
    assert client.post("/tenants", json=body, headers=ACME_OPS).status_code == 403
    assert client.get("/cluster", headers=ACME_OPS).status_code == 403


def test_validation_error_does_not_echo_payload():
    bad = {**JOB, "cron": "nope", "payload": {"phone": "9876543210"}}
    r = client.post("/jobs", json=bad, headers=ADMIN)
    assert r.status_code == 422 and r.json()["error"] == "validation_error"
    assert "9876543210" not in r.text


def test_unexpected_error_is_generic():
    r = client.post("/jobs/1/trigger", headers=ADMIN)
    assert r.status_code == 500
    assert r.json()["error"] == "internal_error" and "request_id" in r.json()
    assert "9876543210" not in r.text and "10.0.0.5" not in r.text
