"""Citywide 'good night / bad night' day-quality read.

Compares a frame's citywide demand against the typical for the SAME weekday and
time-of-day across the month, so a specific Saturday 8 PM is judged against the
month's other Saturday 8 PMs -- a real good-day/bad-day verdict, not peak-vs-offpeak.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from month_day_quality_benchmark import (
    compute_day_quality_typicals,
    current_citywide_pickups,
    day_quality_read,
)


def _store(tmp_path: Path) -> Path:
    store = tmp_path / "exact_shadow.duckdb"
    con = duckdb.connect(str(store))
    con.execute(
        "CREATE TABLE exact_shadow_rows(PULocationID INTEGER, exact_bin_local_ts VARCHAR, pickups_now INTEGER)"
    )
    rows = []
    # Three Saturdays at 20:00 (ISODOW 6, bin 60) with citywide totals 1000/1400/1200.
    for day, total in [("2025-07-05", 1000), ("2025-07-12", 1400), ("2025-07-19", 1200)]:
        for z in range(2, 12):
            rows.append((z, f"{day}T20:00:00", total // 10))
    # A quiet Monday 04:00 (ISODOW 1, bin 12), citywide total 200.
    for z in range(2, 12):
        rows.append((z, "2025-07-14T04:00:00", 20))
    # Airport demand must be excluded from citywide totals.
    rows.append((1, "2025-07-12T20:00:00", 9999))
    con.executemany("INSERT INTO exact_shadow_rows VALUES (?, ?, ?)", rows)
    con.close()
    return store


def test_typicals_are_same_weekday_time_averages(tmp_path: Path):
    con = duckdb.connect(str(_store(tmp_path)), read_only=True)
    try:
        typ = compute_day_quality_typicals(con)
    finally:
        con.close()
    assert typ["6-60"] == pytest.approx(1200.0)  # avg(1000,1400,1200)
    assert typ["1-12"] == pytest.approx(200.0)


def test_airports_excluded_from_current(tmp_path: Path):
    con = duckdb.connect(str(_store(tmp_path)), read_only=True)
    try:
        cur = current_citywide_pickups(con, "2025-07-12T20:00:00")
    finally:
        con.close()
    assert cur == pytest.approx(1400.0)  # the 9999 airport row is not counted


def test_busy_night_reads_high_slow_night_reads_low(tmp_path: Path):
    store = _store(tmp_path)
    con = duckdb.connect(str(store), read_only=True)
    try:
        typ = compute_day_quality_typicals(con)
        busy = current_citywide_pickups(con, "2025-07-12T20:00:00")  # 1400 vs 1200
        slow = current_citywide_pickups(con, "2025-07-05T20:00:00")  # 1000 vs 1200
    finally:
        con.close()
    busy_read = day_quality_read(busy, typ["6-60"], weekday_name="Saturday", time_label="8:00 PM")
    slow_read = day_quality_read(slow, typ["6-60"], weekday_name="Saturday", time_label="8:00 PM")
    assert busy_read["status"] == "ok" and busy_read["score"] > 50 and busy_read["band"] == "high"
    assert slow_read["score"] < 50 and slow_read["band"] == "low"
    assert "Saturday" in busy_read["explain"]


def test_missing_or_zero_typical_is_unavailable():
    assert day_quality_read(500, None)["status"] == "unavailable"
    assert day_quality_read(None, 500)["status"] == "unavailable"
    assert day_quality_read(500, 0)["status"] == "unavailable"


def test_score_is_clamped_and_centered():
    # current == typical -> exactly 50 (a normal night like this).
    assert day_quality_read(1000, 1000)["score"] == 50
    # far above typical saturates at 100, far below floors at 0.
    assert day_quality_read(100000, 1000)["score"] == 100
    assert day_quality_read(0, 1000)["score"] == 0
