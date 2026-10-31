"""Unit tests for pure logic: no DB, no web server."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.core.backoff import backoff_delay
from app.core.fairness import fair_allocate, tenant_demand
from app.core.misfire import COALESCE, FIRE_NOW, SKIP, decide_late_run, plan_materialization
from app.core.partitioning import assign, owner_for, partition_for
from app.core.schedule import classify, fire_times_between, next_fire_times, resolve_local

UTC = timezone.utc
NY = "America/New_York"


# ---------------------------------------------------------------- time zones + DST

def test_kolkata_10pm_roster_is_1630_utc():
    after = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    assert next_fire_times("0 22 * * *", "Asia/Kolkata", after, 1)[0] == datetime(2026, 11, 1, 16, 30, tzinfo=UTC)


def test_dst_gap_fires_once_at_first_valid_time():
    # 2026-03-08: New York clocks jump 02:00 -> 03:00, so 02:30 does not exist.
    after = datetime(2026, 3, 8, 0, 0, tzinfo=UTC)
    times = next_fire_times("30 2 * * *", NY, after, 2)
    assert times[0] == datetime(2026, 3, 8, 7, 0, tzinfo=UTC)          # 03:00 EDT
    assert times[1] == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)         # 02:30 EDT next day


def test_dst_gap_collapses_multiple_cron_times():
    # every 15 min: 02:00, 02:15, 02:30, 02:45 all fall in the gap -> one firing at 03:00
    start = datetime(2026, 3, 8, 6, 30, tzinfo=UTC)   # 01:30 EST
    end = datetime(2026, 3, 8, 7, 30, tzinfo=UTC)     # 03:30 EDT
    times = fire_times_between("*/15 * * * *", NY, start, end)
    local = [t.astimezone(ZoneInfo(NY)).strftime("%H:%M") for t in times]
    assert local == ["01:30", "01:45", "03:00", "03:15", "03:30"]


def test_dst_overlap_fires_once_on_first_occurrence():
    # 2026-11-01: New York clocks fall back 02:00 -> 01:00, so 01:30 happens twice.
    after = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
    times = next_fire_times("30 1 * * *", NY, after, 2)
    assert times[0] == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)        # 01:30 EDT (first)
    assert times[1] == datetime(2026, 11, 2, 6, 30, tzinfo=UTC)        # next day, not 06:30 same day


def test_classify_gap_and_overlap():
    tz = ZoneInfo(NY)
    assert classify(datetime(2026, 3, 8, 2, 30), tz) == "gap"
    assert classify(datetime(2026, 11, 1, 1, 30), tz) == "overlap"
    assert classify(datetime(2026, 6, 1, 1, 30), tz) == "normal"
    assert resolve_local(datetime(2026, 3, 8, 2, 30), tz) == datetime(2026, 3, 8, 7, 0, tzinfo=UTC)


# ---------------------------------------------------------------- misfire

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
LATE = [NOW - timedelta(minutes=m) for m in (30, 20, 10)]
ON_TIME = [NOW - timedelta(seconds=5), NOW + timedelta(seconds=30)]


def test_misfire_fire_now_keeps_all_late_runs():
    keep, skipped = plan_materialization(FIRE_NOW, LATE + ON_TIME, NOW, 60)
    assert keep == LATE + ON_TIME and skipped == 0


def test_misfire_skip_drops_late_runs():
    keep, skipped = plan_materialization(SKIP, LATE + ON_TIME, NOW, 60)
    assert keep == ON_TIME and skipped == 3


def test_misfire_coalesce_keeps_only_latest_late_run():
    keep, skipped = plan_materialization(COALESCE, LATE + ON_TIME, NOW, 60)
    assert keep == [LATE[-1]] + ON_TIME and skipped == 2


def test_decide_late_run():
    assert decide_late_run(SKIP, 5, 60, False) == "RUN"          # within grace: always run
    assert decide_late_run(SKIP, 600, 60, False) == "SKIP"
    assert decide_late_run(COALESCE, 600, 60, True) == "SKIP"    # a newer firing will run
    assert decide_late_run(COALESCE, 600, 60, False) == "RUN"
    assert decide_late_run(FIRE_NOW, 600, 60, True) == "RUN"


# ---------------------------------------------------------------- backoff

def test_backoff_grows_and_caps():
    assert backoff_delay(1, 1, 60, rng=lambda: 1.0) == 1
    assert backoff_delay(3, 1, 60, rng=lambda: 1.0) == 4
    assert backoff_delay(20, 1, 60, rng=lambda: 1.0) == 60
    assert backoff_delay(5, 1, 60, rng=lambda: 0.0) == 0


def test_backoff_jitter_within_bounds():
    for attempt in range(1, 10):
        d = backoff_delay(attempt, 1, 60)
        assert 0 <= d <= min(60, 2 ** (attempt - 1))


# ---------------------------------------------------------------- partitioning

def test_every_partition_has_exactly_one_owner():
    owners = assign(32, ["node1", "node2", "node3"])
    all_parts = sorted(p for ps in owners.values() for p in ps)
    assert all_parts == list(range(32))
    assert all(len(ps) > 0 for ps in owners.values())


def test_removing_a_node_only_moves_its_partitions():
    before = {p: owner_for(p, ["node1", "node2", "node3"]) for p in range(32)}
    after = {p: owner_for(p, ["node1", "node3"]) for p in range(32)}
    moved = [p for p in range(32) if before[p] != after[p]]
    assert all(before[p] == "node2" for p in moved)


def test_partition_for_is_round_robin():
    assert [partition_for(i) for i in (0, 1, 31, 32, 65)] == [0, 1, 31, 0, 1]


# ---------------------------------------------------------------- fairness

def test_small_tenant_is_not_starved_by_big_tenant():
    alloc = fair_allocate(100, {"big": 50_000, "small": 20})
    assert alloc["small"] == 20 and alloc["big"] == 80


def test_fair_allocate_never_exceeds_capacity_or_demand():
    alloc = fair_allocate(10, {"a": 3, "b": 100, "c": 100})
    assert sum(alloc.values()) == 10 and alloc["a"] == 3


def test_tenant_cap_limits_demand():
    demand = tenant_demand({"a": 500, "b": 10}, {"a": 48}, {"a": 50, "b": 50})
    assert demand == {"a": 2, "b": 10}
