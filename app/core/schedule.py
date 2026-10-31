"""Cron evaluation in a tenant's time zone with explicit DST rules. Pure: no DB, no web.

DST policy
- Gap (spring forward, e.g. 02:30 does not exist): fire once at the first valid
  wall-clock minute after the gap (03:00 in New York). Several cron times inside one gap
  collapse into that single firing.
- Overlap (fall back, e.g. 01:30 happens twice): fire once, on the first occurrence
  (fold=0). A wall-clock time never fires twice on the same day.
"""
from datetime import datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

from croniter import croniter

UTC = timezone.utc


def is_valid_cron(expr: str) -> bool:
    try:
        return croniter.is_valid(expr)
    except Exception:
        return False


def is_valid_timezone(name: str) -> bool:
    try:
        ZoneInfo(name)
        return True
    except Exception:
        return False


def _exists(naive: datetime, tz: ZoneInfo) -> bool:
    back = naive.replace(tzinfo=tz).astimezone(UTC).astimezone(tz).replace(tzinfo=None)
    return back == naive


def classify(naive: datetime, tz: ZoneInfo) -> str:
    """'gap', 'overlap' or 'normal' for a local wall-clock time."""
    if not _exists(naive, tz):
        return "gap"
    if naive.replace(tzinfo=tz, fold=0).utcoffset() != naive.replace(tzinfo=tz, fold=1).utcoffset():
        return "overlap"
    return "normal"


def resolve_local(naive: datetime, tz: ZoneInfo) -> datetime:
    """Map a local wall-clock time to a UTC instant using the DST policy above."""
    if _exists(naive, tz):
        return naive.replace(tzinfo=tz, fold=0).astimezone(UTC)
    t = naive.replace(second=0, microsecond=0)
    for _ in range(24 * 60):  # real gaps are at most a couple of hours
        t += timedelta(minutes=1)
        if _exists(t, tz):
            return t.replace(tzinfo=tz).astimezone(UTC)
    raise ValueError(f"could not resolve {naive} in {tz}")


def iter_fire_times(cron: str, tz_name: str, after_utc: datetime) -> Iterator[datetime]:
    """Yield UTC fire instants strictly after `after_utc`, in order, without duplicates."""
    tz = ZoneInfo(tz_name)
    local = after_utc.astimezone(tz).replace(tzinfo=None)
    it = croniter(cron, local)
    last = after_utc
    while True:
        utc = resolve_local(it.get_next(datetime), tz)
        if utc <= last:  # overlap second pass or gap collapse
            continue
        last = utc
        yield utc


def next_fire_times(cron: str, tz_name: str, after_utc: datetime, n: int = 5) -> list[datetime]:
    out = []
    for t in iter_fire_times(cron, tz_name, after_utc):
        out.append(t)
        if len(out) >= n:
            break
    return out


def fire_times_between(cron: str, tz_name: str, start_incl: datetime, end_incl: datetime,
                       limit: int = 10_000) -> list[datetime]:
    out = []
    for t in iter_fire_times(cron, tz_name, start_incl - timedelta(microseconds=1)):
        if t > end_incl or len(out) >= limit:
            break
        out.append(t)
    return out
