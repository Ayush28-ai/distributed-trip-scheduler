"""Prometheus metrics.
- Node metrics (live, per process): served by each node on METRICS_PORT.
- Cluster metrics (from the DB, at scrape time): served by the API at /metrics.
"""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

# ---- per-node (default registry)
FIRE_LAG = Histogram("scheduler_fire_lag_seconds", "Claim time minus scheduled time (first attempt)",
                     buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300))
CLAIMED = Counter("scheduler_runs_claimed_total", "Runs claimed by this node")
SUCCEEDED = Counter("scheduler_runs_succeeded_total", "Runs committed as succeeded")
FAILED = Counter("scheduler_runs_failed_total", "Failed attempts", ["outcome"])  # retry | dead
STALE_REJECTED = Counter("scheduler_stale_commits_rejected_total",
                         "Writes rejected because the fencing token was stale")
LEASE_LOST = Counter("scheduler_leases_lost_total", "Run leases this node failed to renew")
MISFIRE_SKIPPED = Counter("scheduler_misfire_skipped_total", "Runs skipped by misfire policy")
RECLAIMED = Counter("scheduler_runs_reclaimed_total", "Expired runs reset for takeover")
INFLIGHT = Gauge("scheduler_inflight_runs", "Runs executing on this node")
OWNED_PARTITIONS = Gauge("scheduler_owned_partitions", "Partitions leased by this node")


def cluster_metrics_text(stats: dict) -> bytes:
    reg = CollectorRegistry()
    runs = Gauge("cluster_job_runs", "Job runs by status", ["status"], registry=reg)
    for status, n in stats["runs_by_status"].items():
        runs.labels(status).set(n)
    Gauge("cluster_running_jobs", "Runs RUNNING", registry=reg).set(stats["running"])
    Gauge("cluster_retrying_jobs", "Runs RETRY_WAIT", registry=reg).set(stats["retrying"])
    Gauge("cluster_dead_letters", "Dead letters not replayed", registry=reg).set(stats["dead_letters_pending"])
    Gauge("cluster_queue_depth", "Runs due now and not yet claimed", registry=reg).set(stats["queue_depth"])
    Gauge("cluster_oldest_due_age_seconds", "Age of the oldest unclaimed due run",
          registry=reg).set(stats["oldest_due_age_s"])
    Gauge("cluster_error_rate_10m", "Share of runs started in the last 10 min that hit an error",
          registry=reg).set(stats["error_rate_last_10m"])
    lag = Gauge("cluster_fire_lag_ms", "Fire lag percentiles, last 10 min", ["quantile"], registry=reg)
    for q in ("p50", "p95", "p99", "max"):
        if stats["fire_lag_ms_last_10m"][q] is not None:
            lag.labels(q).set(stats["fire_lag_ms_last_10m"][q])
    return generate_latest(reg)
