"""Cloudstream scheduler console: every feature in one Streamlit page.
Talks only to the FastAPI API (auth + tenant isolation enforced there) and, read-only,
to the Spring notification-service for the duplicate check."""
import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import streamlit as st

API = os.getenv("API_URL", "http://localhost:8000")
NOTIFY = os.getenv("NOTIFICATION_URL", "http://localhost:8080")

st.set_page_config(page_title="Cloudstream scheduler", layout="wide")


# ---------------------------------------------------------------- helpers

def call(method: str, path: str, **kw):
    headers = {"Authorization": f"Bearer {st.session_state.get('token', '')}"}
    try:
        r = requests.request(method, f"{API}{path}", headers=headers, timeout=10, **kw)
    except requests.RequestException as e:
        st.error(f"API unreachable: {e}")
        return None
    if r.status_code >= 400:
        st.error(f"{r.status_code}: {r.text[:300]}")
        return None
    return r.json()


def table(rows, cols=None):
    if not rows:
        st.caption("Nothing yet.")
        return
    df = pd.DataFrame(rows)
    st.dataframe(df[cols] if cols else df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------- login

with st.sidebar:
    st.header("Login")
    if "token" not in st.session_state:
        user = st.text_input("Username", "admin")
        pwd = st.text_input("Password", "admin123", type="password")
        if st.button("Sign in", type="primary"):
            r = requests.post(f"{API}/auth/token", json={"username": user, "password": pwd}, timeout=10)
            if r.ok:
                st.session_state.update(token=r.json()["access_token"], role=r.json()["role"],
                                        tenant=r.json()["tenant"], user=user)
                st.rerun()
            else:
                st.error("Bad credentials")
        st.caption("Demo: admin/admin123, acme_ops/acme123 (operator), acme_emp/emp123 (employee)")
        st.stop()
    st.success(f"{st.session_state.user} ({st.session_state.role}, tenant {st.session_state.tenant})")
    if st.button("Sign out"):
        st.session_state.clear()
        st.rerun()
    st.button("Refresh data")

st.title("Cloudstream distributed trip scheduler")
CAN_WRITE = st.session_state.role in ("admin", "operator")
if not CAN_WRITE:
    st.info("Employee view: read-only.")
tabs = st.tabs(["Overview", "Tenants", "Jobs", "Runs", "Dead letters", "Notifications"])

# ---------------------------------------------------------------- overview

with tabs[0]:
    stats = call("GET", "/stats")
    if stats:
        lag = stats["fire_lag_ms_last_10m"]
        c = st.columns(6)
        c[0].metric("Running", stats["running"])
        c[1].metric("Retrying", stats["retrying"])
        c[2].metric("Dead letters", stats["dead_letters_pending"])
        c[3].metric("Done last 60s", stats["succeeded_last_60s"])
        c[4].metric("Fire lag p50 (ms)", lag["p50"] if lag["p50"] is not None else "-")
        c[5].metric("Fire lag p99 (ms)", lag["p99"] if lag["p99"] is not None else "-")
        d = st.columns(3)
        d[0].metric("Queue depth (due, unclaimed)", stats["queue_depth"])
        d[1].metric("Oldest due run (s)", stats["oldest_due_age_s"])
        d[2].metric("Error rate, 10 min", f"{stats['error_rate_last_10m'] * 100:.1f}%")
        st.caption(f"Fire lag over {lag['samples']} runs started in the last 10 minutes")
        st.subheader("Runs by status")
        if stats["runs_by_status"]:
            st.bar_chart(pd.Series(stats["runs_by_status"], name="runs"))
    nodes = call("GET", "/cluster") if st.session_state.role == "admin" else None
    if nodes:
        st.subheader("Cluster: nodes and partition leases")
        table([{**n, "partition_count": len(n["partitions"]),
                "partitions": ", ".join(map(str, n["partitions"]))} for n in nodes],
              ["node_id", "alive", "heartbeat_age_s", "partition_count", "partitions"])

# ---------------------------------------------------------------- tenants

with tabs[1]:
    tenants = call("GET", "/tenants") or []
    table(tenants, ["id", "timezone", "max_concurrency", "paused"])
    for t in tenants:
        col1, col2, col3 = st.columns([3, 1, 1])
        col1.write(f"**{t['id']}** ({t['timezone']}) {'- paused' if t['paused'] else ''}")
        if col2.button("Pause", key=f"p-{t['id']}", disabled=t["paused"] or not CAN_WRITE):
            call("POST", f"/tenants/{t['id']}/pause")
            st.rerun()
        if col3.button("Resume", key=f"r-{t['id']}", disabled=not t["paused"] or not CAN_WRITE):
            call("POST", f"/tenants/{t['id']}/resume")
            st.rerun()
    if st.session_state.role == "admin":
        st.subheader("Create or update tenant")
        with st.form("tenant_form"):
            tid = st.text_input("Tenant id", "acme")
            tz = st.text_input("Time zone", "Asia/Kolkata")
            cap = st.number_input("Max concurrent runs", 1, 10_000, 50)
            if st.form_submit_button("Save tenant"):
                if call("POST", "/tenants", json={"id": tid, "timezone": tz, "max_concurrency": int(cap)}):
                    st.success("Saved")
                    st.rerun()

# ---------------------------------------------------------------- jobs

with tabs[2]:
    st.subheader("Create job")
    tenant_ids = [t["id"] for t in tenants] or [st.session_state.tenant]
    with st.form("job_form"):
        c1, c2, c3 = st.columns(3)
        tenant_id = c1.selectbox("Tenant", tenant_ids)
        job_type = c2.selectbox("Type", ["pickup_alert", "roster", "billing", "no_show", "noop", "flaky"])
        name = c3.text_input("Name", "pickup alert T-100")
        kind = st.radio("Schedule", ["One-off (run in N seconds)", "Recurring (cron)"], horizontal=True)
        c4, c5, c6 = st.columns(3)
        in_s = c4.number_input("Run in seconds (one-off)", 0, 86_400, 10)
        cron = c5.text_input("Cron (recurring)", "*/1 * * * *")
        tz = c6.text_input("Time zone (blank = tenant's)", "")
        c7, c8, c9, c10 = st.columns(4)
        priority = c7.slider("Priority", 0, 10, 5)
        max_runtime = c8.number_input("Max runtime s", 1, 3600, 30)
        attempts = c9.number_input("Max attempts", 1, 20, 5)
        misfire = c10.selectbox("Misfire policy", ["FIRE_NOW", "SKIP", "COALESCE"])
        payload_txt = st.text_input("Payload JSON", '{"trip_id": "T-100", "simulate_ms": 0}')
        if st.form_submit_button("Create job", type="primary", disabled=not CAN_WRITE):
            body = {"tenant_id": tenant_id, "name": name, "job_type": job_type, "priority": priority,
                    "max_runtime_s": int(max_runtime), "max_attempts": int(attempts),
                    "misfire_policy": misfire, "payload": json.loads(payload_txt or "{}")}
            if tz:
                body["timezone"] = tz
            if kind.startswith("One-off"):
                body["run_at"] = (datetime.now(timezone.utc) + timedelta(seconds=int(in_s))).isoformat()
            else:
                body["cron"] = cron
            job = call("POST", "/jobs", json=body)
            if job:
                st.success(f"Created job {job['id']}")

    st.subheader("Jobs")
    jobs = call("GET", "/jobs") or []
    table(jobs, ["id", "tenant_id", "name", "job_type", "cron", "run_at", "timezone", "priority",
                 "misfire_policy", "next_fire_at"])
    if jobs:
        c1, c2 = st.columns(2)
        jid = c1.selectbox("Job", [j["id"] for j in jobs])
        if c1.button("Show next fire times"):
            nf = call("GET", f"/jobs/{jid}/next-fire-times", params={"n": 5})
            if nf:
                st.json(nf)
        if c2.button("Trigger now", disabled=not CAN_WRITE):
            r = call("POST", f"/jobs/{jid}/trigger")
            if r:
                st.success(f"Run {r['run_id']} created")

# ---------------------------------------------------------------- runs

with tabs[3]:
    status = st.selectbox("Status", ["", "SCHEDULED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "DEAD", "SKIPPED"])
    runs = call("GET", "/runs", params={"status": status or None, "limit": 300}) or []
    table(runs, ["id", "job_id", "tenant_id", "name", "job_type", "status", "attempt", "owner_node",
                 "fencing_token", "scheduled_at", "fire_lag_ms", "lease_until", "last_error"])

# ---------------------------------------------------------------- dead letters

with tabs[4]:
    dls = call("GET", "/dead-letters") or []
    table(dls, ["id", "run_id", "tenant_id", "name", "job_type", "attempts", "last_error", "failed_at"])
    if dls:
        dl_id = st.selectbox("Dead letter", [d["id"] for d in dls])
        if st.button("Replay", type="primary", disabled=not CAN_WRITE):
            r = call("POST", f"/dead-letters/{dl_id}/replay")
            if r:
                st.success(f"Run {r['run_id']} re-queued")
                st.rerun()

# ---------------------------------------------------------------- notifications (Spring)

with tabs[5]:
    st.caption("Read from the Spring Boot notification-service. total == distinct keys means no alert was sent twice.")
    nt = st.selectbox("Tenant ", tenant_ids, key="nt")
    try:
        s = requests.get(f"{NOTIFY}/notifications/stats", params={"tenantId": nt}, timeout=20).json()
        c1, c2, c3 = st.columns(3)
        c1.metric("Notifications", s["total"])
        c2.metric("Distinct idempotency keys", s["distinctKeys"])
        c3.metric("Duplicates", s["total"] - s["distinctKeys"])
        rows = requests.get(f"{NOTIFY}/notifications", params={"tenantId": nt, "limit": 200}, timeout=20).json()
        table(rows)
    except Exception as e:
        st.warning(f"notification-service not reachable yet: {e}")
