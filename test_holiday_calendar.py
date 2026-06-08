"""
Tests for holiday_calendar.

The point of these tests is the promise that the closure calendar stays
accurate for *every* year, not just the year it was written: the federal
holidays are computed from date rules off the requested year, never
hardcoded, so this asserts they are correct across the next ~20 years
(2026-2045) including the observed Sat->Fri / Sun->Mon shifts.
"""

import datetime

import holiday_calendar as hc

# The window we explicitly guarantee. Extend freely — the calendar is
# rule-based, so this is just the range the test actively pins down.
YEARS = range(2026, 2046)


# Independent re-derivation of the rules (kept separate from the module so
# a bug in the module can't hide behind a shared helper).
def _nth(y, m, wd, n):
    first = datetime.date(y, m, 1)
    return first + datetime.timedelta(days=(wd - first.weekday()) % 7 + 7 * (n - 1))


def _last(y, m, wd):
    nxt = datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1)
    last = nxt - datetime.timedelta(days=1)
    return last - datetime.timedelta(days=(last.weekday() - wd) % 7)


def _obs(d):
    if d.weekday() == 5:
        return d - datetime.timedelta(days=1)
    if d.weekday() == 6:
        return d + datetime.timedelta(days=1)
    return d


def _expected(y):
    return {
        _obs(datetime.date(y, 1, 1)),     # New Year's Day
        _nth(y, 1, 0, 3),                 # MLK Day
        _nth(y, 2, 0, 3),                 # Presidents' Day
        _last(y, 5, 0),                   # Memorial Day
        _obs(datetime.date(y, 6, 19)),    # Juneteenth
        _obs(datetime.date(y, 7, 4)),     # Independence Day
        _nth(y, 9, 0, 1),                 # Labor Day
        _nth(y, 10, 0, 2),                # Columbus / Indigenous Peoples' Day
        _obs(datetime.date(y, 11, 11)),   # Veterans Day
        _nth(y, 11, 3, 4),                # Thanksgiving
        _obs(datetime.date(y, 12, 25)),   # Christmas
    }


def test_federal_holidays_accurate_for_20_years():
    for y in YEARS:
        got = hc.federal_holidays(y)
        assert len(got) == 11, f"{y}: expected 11 holidays, got {len(got)}"
        assert set(got) == _expected(y), f"{y}: holiday set mismatch"
        assert got == sorted(got), f"{y}: result is not sorted"


def test_no_observed_holiday_falls_on_a_weekend():
    # The observed shift exists precisely so offices never "close" on a day
    # that is already a weekend; assert it holds every year.
    for y in YEARS:
        for d in hc.federal_holidays(y):
            assert d.weekday() < 5, f"{y}: {d.isoformat()} lands on a weekend"


def test_monday_and_thursday_holidays_land_on_the_right_weekday():
    for y in YEARS:
        hs = set(hc.federal_holidays(y))
        for monday in (_nth(y, 1, 0, 3), _nth(y, 2, 0, 3), _last(y, 5, 0),
                       _nth(y, 9, 0, 1), _nth(y, 10, 0, 2)):
            assert monday in hs and monday.weekday() == 0
        thanksgiving = _nth(y, 11, 3, 4)
        assert thanksgiving in hs and thanksgiving.weekday() == 3


def test_known_observed_shifts():
    # Jul 4 2026 is a Saturday -> observed Friday Jul 3.
    assert datetime.date(2026, 7, 3) in hc.federal_holidays(2026)
    # Jul 4 2027 is a Sunday -> observed Monday Jul 5.
    assert datetime.date(2027, 7, 5) in hc.federal_holidays(2027)
    # Dec 25 2027 is a Saturday -> observed Friday Dec 24.
    assert datetime.date(2027, 12, 24) in hc.federal_holidays(2027)
    # Jul 4 2033 is a Monday -> no shift.
    assert datetime.date(2033, 7, 4) in hc.federal_holidays(2033)


def test_calendar_payload_shape_and_two_year_coverage():
    cal = hc.calendar_payload(datetime.date(2026, 6, 8))
    assert cal["tz"] == "America/New_York"
    assert isinstance(cal["holidays"], list) and cal["holidays"]
    for s in cal["holidays"]:                 # every holiday is ISO YYYY-MM-DD
        datetime.date.fromisoformat(s)
    assert any(s.startswith("2026-") for s in cal["holidays"])  # this year
    assert any(s.startswith("2027-") for s in cal["holidays"])  # and next
    assert any(s.startswith("2028-") for s in cal["holidays"])  # and the year after
    school = cal["seasonal_closures"]["private_school"]
    assert school and all(len(r) == 2 for r in school)
    for a, b in school:                       # explicit ISO [start, end] ranges
        assert datetime.date.fromisoformat(a) <= datetime.date.fromisoformat(b)


def test_payload_is_json_serializable():
    import json
    json.dumps(hc.calendar_payload(datetime.date(2030, 1, 1)))


def test_easter_computus_known_dates():
    assert hc._easter(2024) == datetime.date(2024, 3, 31)
    assert hc._easter(2025) == datetime.date(2025, 4, 20)
    assert hc._easter(2026) == datetime.date(2026, 4, 5)
    assert hc._easter(2027) == datetime.date(2027, 3, 28)


def test_school_closures_track_each_year():
    for y in YEARS:
        ranges = hc.school_closures(y)
        for start, end in ranges:
            assert isinstance(start, datetime.date) and isinstance(end, datetime.date)
            assert start <= end
        # summer recess covers mid-July; winter covers Dec 31
        assert any(s <= datetime.date(y, 7, 15) <= e for s, e in ranges)
        assert any(s <= datetime.date(y, 12, 31) <= e for s, e in ranges)
        # midwinter recess includes Presidents' Day (3rd Mon Feb)
        pres = _nth(y, 2, 0, 3)
        assert any(s <= pres <= e for s, e in ranges)
