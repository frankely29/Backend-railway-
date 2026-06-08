"""
Built-in NYC closure calendar for the long-trip dollar flags.

Each dollar flag carries a per-category time-of-day schedule (peak / off /
prime) plus a `weekday_only` flag. That covers the *clock* but not the
*calendar*: an office tower is just as closed on Thanksgiving as on a
Sunday, and the elite-school flag is dead all summer and over every recess.
This module supplies that date dimension.

Everything is computed from date arithmetic — no external service, no
`holidays` package — so it is deterministic and works offline (matching the
recompute-on-read design of the hotspots endpoint). The output is a small
JSON-able payload served alongside GET /long_trip_hotspots; the frontend,
which already evaluates the time-of-day schedule against its own NYC clock,
checks today's date against this payload to decide whether a weekday-only /
seasonal flag is closed (dimmed, and never pulsing).

Holidays are the US federal set with the standard observed shift (a holiday
on Saturday is observed the preceding Friday, on Sunday the following
Monday) — that observed date is when offices actually close. School ranges
follow the NYC DOE calendar pattern (summer recess after late June, plus
winter / midwinter / spring breaks) and are intentionally approximate: they
drift a few days year to year, which is fine for a heuristic flag.
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Optional


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> datetime.date:
    """The nth (1-based) `weekday` (Mon=0 .. Sun=6) of month/year."""
    first = datetime.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + datetime.timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> datetime.date:
    """The last `weekday` (Mon=0 .. Sun=6) of month/year."""
    nxt = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1))
    last = nxt - datetime.timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - datetime.timedelta(days=offset)


def _observed(d: datetime.date) -> datetime.date:
    """Federal observed-date shift for a fixed-date holiday."""
    if d.weekday() == 5:        # Saturday -> observed Friday
        return d - datetime.timedelta(days=1)
    if d.weekday() == 6:        # Sunday -> observed Monday
        return d + datetime.timedelta(days=1)
    return d


def federal_holidays(year: int) -> List[datetime.date]:
    """
    US federal holidays for `year`, as the dates offices observe them.
    Fixed-date holidays are shifted to the nearest weekday (the day the
    holiday is actually taken off); the Monday holidays are already on a
    weekday by construction.
    """
    return sorted({
        _observed(datetime.date(year, 1, 1)),     # New Year's Day
        _nth_weekday(year, 1, 0, 3),               # MLK Day (3rd Mon Jan)
        _nth_weekday(year, 2, 0, 3),               # Presidents' Day (3rd Mon Feb)
        _last_weekday(year, 5, 0),                 # Memorial Day (last Mon May)
        _observed(datetime.date(year, 6, 19)),     # Juneteenth
        _observed(datetime.date(year, 7, 4)),      # Independence Day
        _nth_weekday(year, 9, 0, 1),               # Labor Day (1st Mon Sep)
        _nth_weekday(year, 10, 0, 2),              # Columbus / Indigenous Peoples' Day
        _observed(datetime.date(year, 11, 11)),    # Veterans Day
        _nth_weekday(year, 11, 3, 4),              # Thanksgiving (4th Thu Nov)
        _observed(datetime.date(year, 12, 25)),    # Christmas
    })


# School-year recess ranges as recurring [start_md, end_md] (inclusive,
# "MM-DD"). The elite private-school flag is closed across these in addition
# to weekends + federal holidays. Modeled on the NYC DOE calendar; a winter
# range wraps the Dec -> Jan boundary (start_md > end_md). Approximate by
# design — spring recess in particular moves with Passover/Easter.
SCHOOL_CLOSED_RANGES: List[List[str]] = [
    ["06-27", "09-07"],   # summer recess (after ~last day of school)
    ["12-24", "01-02"],   # winter recess (wraps year boundary)
    ["02-14", "02-22"],   # midwinter / Presidents'-Day week
    ["04-01", "04-13"],   # spring recess (approx)
]


def calendar_payload(today: Optional[datetime.date] = None) -> Dict[str, object]:
    """
    JSON-able closure calendar for the frontend. `holidays` covers this year
    and next, so the list stays valid across the New-Year boundary that a
    poll might straddle. The frontend matches its NYC date against
    `holidays` (weekday-only flags) and `seasonal_closures` (per category).
    """
    if today is None:
        today = datetime.date.today()
    holidays: List[str] = []
    for y in (today.year, today.year + 1):
        holidays.extend(d.isoformat() for d in federal_holidays(y))
    return {
        "tz": "America/New_York",
        "holidays": holidays,
        # Keyed by the flag's dominant category. Weekday-only categories
        # already go dark on weekends + holidays via the weekday_only rule;
        # these add the longer recurring seasonal closures on top.
        "seasonal_closures": {
            "private_school": [list(r) for r in SCHOOL_CLOSED_RANGES],
        },
    }
