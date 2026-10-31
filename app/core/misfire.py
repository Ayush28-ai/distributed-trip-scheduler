"""Misfire policies. Pure: no DB, no web.

A firing is "late" when it is more than `grace_s` behind its scheduled time
(cluster was down, tenant was paused, backlog).
- FIRE_NOW: every late firing still runs, as soon as possible (catch-up, capped at MAX_CATCHUP).
- SKIP:     late firings are dropped; the job continues from the next on-time firing.
- COALESCE: all late firings of a job collapse into one run (the most recent one).
"""
from datetime import datetime

FIRE_NOW, SKIP, COALESCE = "FIRE_NOW", "SKIP", "COALESCE"
POLICIES = (FIRE_NOW, SKIP, COALESCE)
MAX_CATCHUP = 100


def plan_materialization(policy: str, fire_times: list[datetime], now: datetime,
                         grace_s: float) -> tuple[list[datetime], int]:
    """Which of `fire_times` (sorted) become job_runs. Returns (keep, skipped_count)."""
    late = [t for t in fire_times if (now - t).total_seconds() > grace_s]
    on_time = [t for t in fire_times if (now - t).total_seconds() <= grace_s]
    if policy == SKIP:
        keep_late = []
    elif policy == COALESCE:
        keep_late = late[-1:]
    else:
        keep_late = late[-MAX_CATCHUP:]
    return keep_late + on_time, len(late) - len(keep_late)


def decide_late_run(policy: str, lag_s: float, grace_s: float, has_newer_due: bool) -> str:
    """For a run already materialized and just claimed: 'RUN' or 'SKIP'."""
    if lag_s <= grace_s:
        return "RUN"
    if policy == SKIP:
        return "SKIP"
    if policy == COALESCE and has_newer_due:
        return "SKIP"
    return "RUN"
