"""The month-anchor post-pass, executed on a real DuckDB table.

Simulates the flaw (per-frame ranking makes the top zone green on BOTH a busy and
a quiet day) and asserts the whole-month re-base fixes it: a quiet day never
reaches green, a busy day does, order is preserved, airports are untouched.
"""
from __future__ import annotations

import duckdb
import pytest

from month_color_benchmark import month_anchor_citywide_update_sql


def _seed(con):
    con.execute(
        """
        CREATE TABLE exact_shadow_rows (
          PULocationID INTEGER,
          exact_bin_local_ts TIMESTAMP,
          pickups_now DOUBLE,
          earnings_shadow_rating_citywide_v3 INTEGER,
          earnings_shadow_bucket_citywide_v3 VARCHAR,
          earnings_shadow_color_citywide_v3 VARCHAR
        )
        """
    )
    rows = []
    # BUSY day frame: high pickups (100..300). Per-frame rank made them all green.
    for i, z in enumerate(range(10, 21)):
        rows.append((z, "2025-07-05 20:00:00", 100.0 + i * 20, 90, "green", "#00b050"))
    # QUIET day frame: tiny pickups (1..21). Per-frame rank ALSO made the top green
    # (the flaw). After the re-base these must NOT be green.
    for i, z in enumerate(range(10, 21)):
        rows.append((z, "2025-07-08 04:00:00", 1.0 + i * 2, 88, "green", "#00b050"))
    # Airport zone (132) with huge pickups — must be left untouched.
    rows.append((132, "2025-07-05 20:00:00", 500.0, 0, "red", "#e60000"))
    con.executemany(
        "INSERT INTO exact_shadow_rows VALUES (?, ?, ?, ?, ?, ?)", rows
    )


def _bucket(con, zone, ts):
    return con.execute(
        "SELECT earnings_shadow_rating_citywide_v3, earnings_shadow_bucket_citywide_v3 "
        "FROM exact_shadow_rows WHERE PULocationID=? AND exact_bin_local_ts=?",
        [zone, ts],
    ).fetchone()


def test_quiet_day_loses_green_busy_day_keeps_it_airport_untouched():
    con = duckdb.connect()
    _seed(con)
    con.execute(month_anchor_citywide_update_sql())

    # Quiet 4am top zone (highest pickups that frame = zone 20, 21 pickups) must
    # NOT be green anymore — it's low against the whole month.
    r_quiet, b_quiet = _bucket(con, 20, "2025-07-08 04:00:00")
    assert b_quiet != "green", (r_quiet, b_quiet)

    # Busy 8pm top zone (zone 20, 300 pickups) should reach green.
    r_busy, b_busy = _bucket(con, 20, "2025-07-05 20:00:00")
    assert b_busy == "green", (r_busy, b_busy)

    # A quiet-day zone scores LOWER than the same-rank busy-day zone — consistency.
    assert r_quiet < r_busy

    # Airport zone 132 is untouched (still its seeded 0/red).
    r_air, b_air = _bucket(con, 132, "2025-07-05 20:00:00")
    assert (r_air, b_air) == (0, "red")


def test_order_within_a_frame_is_preserved():
    con = duckdb.connect()
    _seed(con)
    con.execute(month_anchor_citywide_update_sql())
    ratings = [
        _bucket(con, z, "2025-07-05 20:00:00")[0] for z in range(10, 21)
    ]
    # pickups increase with zone id, so ratings must be non-decreasing.
    assert ratings == sorted(ratings)


def test_color_matches_bucket_after_rebase():
    con = duckdb.connect()
    _seed(con)
    con.execute(month_anchor_citywide_update_sql())
    mismatched = con.execute(
        """
        SELECT COUNT(*) FROM exact_shadow_rows
        WHERE (earnings_shadow_bucket_citywide_v3 = 'green') <> (earnings_shadow_color_citywide_v3 = '#00b050')
        """
    ).fetchone()[0]
    assert mismatched == 0
