"""
Built-in NYC closure calendar for the long-trip dollar flags.

Each dollar flag carries a per-category time-of-day schedule (peak / off /
prime) plus a `weekday_only` flag. That covers the *clock* but not the
*calendar*: an office tower is just as closed on Thanksgiving as on a
Sunday, and the elite-school flag is dead all summer and over every recess.
This module supplies that date dimension.

Everything is computed from date arithmetic — no external service, no
`holidays` package — so it is deterministic and works offline (matching the
recompute-on-read design of the hotspots endpoint), and it stays correct
for *any* year rather than a hardcoded one. The output is a small JSON-able
payload served alongside GET /long_trip_hotspots; the frontend, which
already evaluates the time-of-day schedule against its own NYC clock,
checks today's date against this payload to decide whether a weekday-only /
seasonal flag is closed (dimmed, and never pulsing).

Holidays are the US federal set with the standard observed shift (a holiday
on Saturday is observed the preceding Friday, on Sunday the following
Monday) — that observed date is when offices actually close. School recess
ranges follow the NYC DOE calendar pattern and are computed per year:
summer (anchored to Labor Day), winter, midwinter (Presidents'-Day week),
and spring. Spring recess is the one piece NYC sets by Passover/Easter with
no published 20-years-out formula, so it uses the week of Good Friday
(computed via the Gregorian Computus) as a best-effort proxy.
"""

from __future__ import annotations

import datetime
from typing import Dict, List, Optional, Tuple


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


def _easter(year: int) -> datetime.date:
    """Western (Gregorian) Easter Sunday — the Anonymous Gregorian Computus."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ll = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ll) // 451
    month = (h + ll - 7 * m + 114) // 31
    day = ((h + ll - 7 * m + 114) % 31) + 1
    return datetime.date(year, month, day)


def _mon_to_fri_week(d: datetime.date) -> Tuple[datetime.date, datetime.date]:
    """The Monday..Friday of the week containing date `d`."""
    monday = d - datetime.timedelta(days=d.weekday())
    return monday, monday + datetime.timedelta(days=4)


# Exact NYC DOE spring-recess dates where published (spring is the one
# recess NYC ties to Passover/Easter, which the Good-Friday-week proxy
# can't always nail — e.g. Passover-extended years). Extend as the DOE
# publishes; unlisted years fall back to the computed proxy.
_SPRING_OVERRIDES: Dict[int, Tuple[str, str]] = {
    2025: ("2025-04-14", "2025-04-18"),
    2026: ("2026-04-02", "2026-04-10"),
}


def school_closures(year: int) -> List[Tuple[datetime.date, datetime.date]]:
    """
    NYC-DOE-style school recess ranges (inclusive `datetime.date` pairs) for
    the school activity in `year`. Computed per year so they track the
    actual calendar rather than drifting:
      - summer: ~late June through the day before school resumes (the
        Wednesday after Labor Day);
      - winter: Dec 24 (year) through Jan 1 (year + 1);
      - midwinter: the Mon–Fri week of Presidents' Day (3rd Mon Feb);
      - spring: the Mon–Fri week of Good Friday (Easter − 2). NYC ties
        spring recess to Passover/Easter; there is no published formula
        20 years out, so this is a best-effort proxy.
    """
    labor_day = _nth_weekday(year, 9, 0, 1)
    midwinter = _mon_to_fri_week(_nth_weekday(year, 2, 0, 3))   # Presidents' week
    ov = _SPRING_OVERRIDES.get(year)
    if ov:
        spring = (datetime.date.fromisoformat(ov[0]), datetime.date.fromisoformat(ov[1]))
    else:
        # Good-Friday week (Easter − 2). Exact for typical single-week
        # recesses; Passover-extended years are pinned in _SPRING_OVERRIDES.
        spring = _mon_to_fri_week(_easter(year) - datetime.timedelta(days=2))
    return [
        (datetime.date(year, 6, 27), labor_day + datetime.timedelta(days=2)),  # summer
        (datetime.date(year, 12, 24), datetime.date(year + 1, 1, 1)),          # winter
        midwinter,
        spring,
    ]


def calendar_payload(today: Optional[datetime.date] = None) -> Dict[str, object]:
    """
    JSON-able closure calendar for the frontend. The frontend matches its
    NYC date against `holidays` (weekday-only flags) and `seasonal_closures`
    (per category, explicit `[start, end]` ISO date ranges).

    Holidays span this year through year + 2 so the list stays valid across
    the New-Year boundary even for an observed holiday that belongs to a
    later year but lands on Dec 31 (e.g. New Year's Day observed on the
    preceding Friday). School ranges span year − 1 .. year + 1 so a date in
    early January still sees the winter recess that began the prior December.
    """
    if today is None:
        today = datetime.date.today()

    holidays = sorted({
        d.isoformat()
        for y in (today.year, today.year + 1, today.year + 2)
        for d in federal_holidays(y)
    })

    school: List[List[str]] = []
    seen = set()
    for y in (today.year - 1, today.year, today.year + 1):
        for start, end in school_closures(y):
            key = (start.isoformat(), end.isoformat())
            if key not in seen:
                seen.add(key)
                school.append([key[0], key[1]])
    school.sort()

    return {
        "tz": "America/New_York",
        "holidays": holidays,
        # Keyed by the flag's dominant category. Weekday-only categories
        # already go dark on weekends + holidays via the weekday_only rule;
        # these add the longer recurring seasonal closures on top.
        "seasonal_closures": {"private_school": school},
    }
