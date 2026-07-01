"""Tendency's citywide (per-borough) families read the SAME month demand
benchmark as the map colors, not the old per-frame rank average.

The old Tendency averaged the stored per-frame rank (earnings_shadow_rating_*),
which re-normalizes every 20 minutes -- so every zone trended toward ~the same
value and Tendency could disagree with the colors. The unified version scores
each zone-frame's absolute month demand percentile (PERCENT_RANK over
LN(1+pickups_now), airports excluded) -- the exact twin of the color
breakpoints -- then averages per borough.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from month_tendency_benchmark import build_month_tendency_benchmark

_RATING_COLS = [
    "earnings_shadow_rating_citywide_v3", "earnings_shadow_rating_citywide_v2",
    "earnings_shadow_rating_manhattan_v3", "earnings_shadow_rating_manhattan_v2",
    "earnings_shadow_rating_bronx_wash_heights_v3", "earnings_shadow_rating_bronx_wash_heights_v2",
    "earnings_shadow_rating_queens_v3", "earnings_shadow_rating_queens_v2",
    "earnings_shadow_rating_brooklyn_v3", "earnings_shadow_rating_brooklyn_v2",
    "earnings_shadow_rating_staten_island_v3", "earnings_shadow_rating_staten_island_v2",
]


def _make_store_and_zones(tmp_path: Path):
    store = tmp_path / "exact_shadow.duckdb"
    con = duckdb.connect(str(store))
    cols = (
        "PULocationID INTEGER, exact_bin_local_ts VARCHAR, "
        "earnings_shadow_score_citywide_v3_anchor_shadow DOUBLE, "
        + ", ".join(f"{c} DOUBLE" for c in _RATING_COLS)
    )
    con.execute(f"CREATE TABLE exact_shadow_rows({cols})")
    # Two Manhattan zones with very different EARNINGS score but IDENTICAL stored
    # per-frame rank (90) -- the old flaw. The month benchmark must separate them
    # on the earnings score (which carries saturation + all formulas).
    rows = []
    const_ratings = [90.0] * len(_RATING_COLS)
    for ts in range(60):
        rows.append((100, f"t{ts}", 0.85, *const_ratings))  # high earnings quality
        rows.append((200, f"t{ts}", 0.05, *const_ratings))  # low earnings quality
    placeholders = ",".join(["?"] * (3 + len(_RATING_COLS)))
    con.executemany(f"INSERT INTO exact_shadow_rows VALUES ({placeholders})", rows)
    con.close()

    zones = tmp_path / "zones.geojson"
    zones.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"properties": {"LocationID": 100, "zone": "Busy", "borough": "Manhattan"},
         "geometry": {"type": "Point", "coordinates": [-73.98, 40.75]}},
        {"properties": {"LocationID": 200, "zone": "Quiet", "borough": "Manhattan"},
         "geometry": {"type": "Point", "coordinates": [-73.98, 40.75]}},
    ]}))
    return store, zones


def test_citywide_family_uses_demand_benchmark_not_perframe_rank(tmp_path: Path):
    store, zones = _make_store_and_zones(tmp_path)
    out = build_month_tendency_benchmark(
        exact_store_path=store, zones_geojson_path=zones, month_key="2025-07"
    )
    assert out["version"] == "month_tendency_benchmark_v3"
    fam = out["families"]["citywide_all"]
    # If it still averaged the per-frame rank it would be ~90. Demand-anchored, a
    # busy+quiet mix averages far below that.
    assert fam["average_rating"] is not None
    assert abs(fam["average_rating"] - 90.0) > 10.0
    assert fam["sample_zone_frames"] == 120


def test_per_borough_families_present(tmp_path: Path):
    store, zones = _make_store_and_zones(tmp_path)
    out = build_month_tendency_benchmark(
        exact_store_path=store, zones_geojson_path=zones, month_key="2025-07"
    )
    fams = out["families"]
    # Per-borough breakdown is preserved (the user's requirement: stay per-borough).
    for key in ("citywide_all", "auto_manhattan_citywide", "auto_queens_citywide",
                "auto_brooklyn_citywide", "auto_bronx_wash_heights_citywide"):
        assert key in fams
    assert fams["auto_manhattan_citywide"]["average_rating"] is not None
