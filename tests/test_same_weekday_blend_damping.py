"""Tests for the system-wide same-weekday blend + volatility damping.

The blend turns each single-day 20-minute frame into a weighted same-weekday
average (today + up to 3 prior weeks), and the damping shaves up to MAX_DAMP
off a zone's score/rating when its same-weekday demand is inconsistent (busy
some weeks, dead others). Both are applied to EVERY user-visible color mode.

The unit tests exercise the pure blend/damp math directly (deterministic, no
parquet/timezone dependence). The contract test runs the real engine SQL to
prove every blend/damp column actually exists in the output (so the
prior-week SELECT can never reference a missing column). The end-to-end test
drives build_single_frame_for_month and asserts a zone that spikes one week
but is dead the prior weeks ends up rated below a steady zone.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb
import pytest

import build_hotspot as bh
from build_hotspot import (
    SAME_WEEKDAY_BLEND_COLUMNS,
    SAME_WEEKDAY_CONSISTENCY_MAX_DAMP,
    SAME_WEEKDAY_DAMP_TARGET_COLUMNS,
    _apply_blended_columns_to_row_map,
    _blend_same_weekday_rows,
    _same_weekday_consistency_damp,
)
from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_mode_profiles import ZONE_MODE_PROFILES

TODAY = "2025-01-28T18:00:00"
PRIORS = ["2025-01-21T18:00:00", "2025-01-14T18:00:00", "2025-01-07T18:00:00"]

# Synthetic blend layout for the unit tests: three damp targets spanning three
# different modes (a citywide score, a borough raw score, the 45+ rating) plus
# a confidence column that must NOT be damped. pickups_now is appended last.
_COLS = (
    "earnings_shadow_score_citywide_v3_anchor_shadow",
    "earnings_shadow_score_raw_manhattan_v3",
    "earnings_shadow_rating_trips_45plus_v3",
    "earnings_shadow_confidence_citywide_v3",
)
_DAMP = (
    "earnings_shadow_score_citywide_v3_anchor_shadow",
    "earnings_shadow_score_raw_manhattan_v3",
    "earnings_shadow_rating_trips_45plus_v3",
)
_CONSISTENCY_INDEX = len(_COLS)  # pickups_now sits right after the blend columns


def _entries(score_by_week, pickups_by_week):
    """today + up to 3 priors, each (ts, (score, score, score*100, 0.9, pickups))."""
    times = [TODAY] + PRIORS
    out = []
    for ts, score, pk in zip(times, score_by_week, pickups_by_week):
        out.append((ts, (score, score, None if score is None else score * 100.0, 0.9, pk)))
    return out


# --------------------------------------------------------------------------- #
# Pure blend + damp math
# --------------------------------------------------------------------------- #
def test_blend_damps_all_modes_uniformly_and_spares_confidence() -> None:
    by_zone = {
        # Spiky: identical scores every week (so the pre-damp blend equals
        # today's value), but pickups spike one week and are dead the others ->
        # high CoV -> damped.
        1: _entries([0.8, 0.8, 0.8, 0.8], [40.0, 0.0, 0.0, 0.0]),
        # Steady: identical scores AND steady pickups -> CoV 0 -> undamped.
        2: _entries([0.8, 0.8, 0.8, 0.8], [10.0, 10.0, 10.0, 10.0]),
    }
    out = _blend_same_weekday_rows(
        by_zone, TODAY, _COLS, consistency_index=_CONSISTENCY_INDEX, damp_columns=_DAMP
    )
    spiky, steady = out[1], out[2]

    # Steady zone is left exactly where the weighted average put it.
    assert steady["earnings_shadow_score_citywide_v3_anchor_shadow"] == pytest.approx(0.8)
    assert steady["earnings_shadow_score_raw_manhattan_v3"] == pytest.approx(0.8)
    assert steady["earnings_shadow_rating_trips_45plus_v3"] == pytest.approx(80.0)

    # Spiky zone: pickups CoV here is >= CV_HI, so the full MAX_DAMP applies to
    # every damp-target mode by the SAME factor (one per-zone signal).
    factor = 1.0 - SAME_WEEKDAY_CONSISTENCY_MAX_DAMP
    for col, steady_val in (
        ("earnings_shadow_score_citywide_v3_anchor_shadow", 0.8),
        ("earnings_shadow_score_raw_manhattan_v3", 0.8),
        ("earnings_shadow_rating_trips_45plus_v3", 80.0),
    ):
        assert spiky[col] == pytest.approx(steady_val * factor)
        assert spiky[col] < steady[col]
        assert spiky[col] / steady[col] == pytest.approx(factor)

    # Confidence is blended but never damped.
    assert spiky["earnings_shadow_confidence_citywide_v3"] == pytest.approx(0.9)
    assert steady["earnings_shadow_confidence_citywide_v3"] == pytest.approx(0.9)


def test_blend_pulls_spiky_zone_below_today_then_damps() -> None:
    # The user's exact scenario: busy today, near-dead the prior 3 weeks.
    by_zone = {
        1: _entries([0.9, 0.1, 0.1, 0.1], [50.0, 1.0, 1.0, 1.0]),
    }
    out = _blend_same_weekday_rows(
        by_zone, TODAY, _COLS, consistency_index=_CONSISTENCY_INDEX, damp_columns=_DAMP
    )
    anchor = out[1]["earnings_shadow_score_citywide_v3_anchor_shadow"]
    pre_damp = 0.4 * 0.9 + 0.2 * 0.1 * 3  # weighted same-weekday average == 0.42
    assert pre_damp == pytest.approx(0.42)
    # The blend already pulls today's 0.9 down to 0.42; damping drops it further.
    assert anchor < pre_damp
    assert anchor < 0.9


def test_blend_cold_start_today_only_is_unchanged_and_undamped() -> None:
    by_zone = {1: [(TODAY, (0.7, 0.5, 60.0, 0.9, 40.0))]}
    out = _blend_same_weekday_rows(
        by_zone, TODAY, _COLS, consistency_index=_CONSISTENCY_INDEX, damp_columns=_DAMP
    )
    assert out[1]["earnings_shadow_score_citywide_v3_anchor_shadow"] == pytest.approx(0.7)
    assert out[1]["earnings_shadow_score_raw_manhattan_v3"] == pytest.approx(0.5)
    assert out[1]["earnings_shadow_rating_trips_45plus_v3"] == pytest.approx(60.0)


def test_blend_drops_zone_without_a_today_sample() -> None:
    by_zone = {1: [(PRIORS[0], (0.5, 0.5, 50.0, 0.9, 10.0))]}
    out = _blend_same_weekday_rows(
        by_zone, TODAY, _COLS, consistency_index=_CONSISTENCY_INDEX, damp_columns=_DAMP
    )
    assert 1 not in out


def test_blend_handles_none_values_without_nan() -> None:
    by_zone = {
        1: [
            (TODAY, (None, 0.5, None, None, None)),
            (PRIORS[0], (0.3, None, 30.0, 0.8, 5.0)),
        ],
    }
    out = _blend_same_weekday_rows(
        by_zone, TODAY, _COLS, consistency_index=_CONSISTENCY_INDEX, damp_columns=_DAMP
    )
    assert 1 in out
    for value in out[1].values():
        assert math.isfinite(value)


def test_consistency_damp_curve_edges_and_bounds() -> None:
    f = _same_weekday_consistency_damp
    assert f([]) == 0.0
    assert f([5.0]) == 0.0  # < 2 samples (cold start)
    assert f([10.0, 10.0, 10.0]) == 0.0  # CoV == 0 (perfectly reliable)
    assert f([0.0, 0.0, 0.0]) == 0.0  # mean == 0
    # A one-week spike with dead priors saturates to the full cap.
    assert f([40.0, 0.0, 0.0, 0.0]) == pytest.approx(SAME_WEEKDAY_CONSISTENCY_MAX_DAMP)
    # Everything stays inside [0, MAX_DAMP] and finite.
    for samples in ([1, 2, 3, 4], [5, 5, 6, 4], [100, 1], [3, 3, 3, 9]):
        d = f([float(x) for x in samples])
        assert math.isfinite(d)
        assert 0.0 <= d <= SAME_WEEKDAY_CONSISTENCY_MAX_DAMP
    # More spread -> at least as much damping.
    assert f([11.0, 10.0, 9.0, 10.0]) <= f([40.0, 0.0, 0.0, 0.0])


def test_apply_blended_columns_rounds_only_rating_fields() -> None:
    row = {
        "earnings_shadow_rating_trips_45plus_v3": 80,
        "earnings_shadow_score_citywide_v3_anchor_shadow": 0.5,
    }
    _apply_blended_columns_to_row_map(
        row,
        {
            "earnings_shadow_rating_trips_45plus_v3": 63.6,
            "earnings_shadow_score_citywide_v3_anchor_shadow": 0.41,
        },
    )
    assert row["earnings_shadow_rating_trips_45plus_v3"] == 64
    assert isinstance(row["earnings_shadow_rating_trips_45plus_v3"], int)
    assert row["earnings_shadow_score_citywide_v3_anchor_shadow"] == pytest.approx(0.41)
    # A None payload is a no-op.
    _apply_blended_columns_to_row_map(row, None)
    assert row["earnings_shadow_rating_trips_45plus_v3"] == 64


# --------------------------------------------------------------------------- #
# Contract: every blend/damp column is a real engine output column
# --------------------------------------------------------------------------- #
def _shadow_profile_kwargs() -> dict:
    return dict(
        profile=ZONE_MODE_PROFILES["citywide_v2"],
        citywide_v3_profile=ZONE_MODE_PROFILES["citywide_v3"],
        manhattan_profile=ZONE_MODE_PROFILES["manhattan_v2"],
        bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v2"],
        queens_profile=ZONE_MODE_PROFILES["queens_v2"],
        brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v2"],
        staten_island_profile=ZONE_MODE_PROFILES["staten_island_v2"],
        manhattan_v3_profile=ZONE_MODE_PROFILES["manhattan_v3"],
        bronx_wash_heights_v3_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v3"],
        queens_v3_profile=ZONE_MODE_PROFILES["queens_v3"],
        brooklyn_v3_profile=ZONE_MODE_PROFILES["brooklyn_v3"],
        staten_island_v3_profile=ZONE_MODE_PROFILES["staten_island_v3"],
        trips_45plus_v3_profile=ZONE_MODE_PROFILES["trips_45plus_v3"],
        available_columns={"request_datetime", "shared_match_flag", "shared_request_flag"},
    )


def _write_tiny_parquet(path: Path) -> None:
    con = duckdb.connect(database=":memory:")
    con.execute(
        """
        CREATE TABLE trips AS
        SELECT * FROM (
            VALUES
                (46, 50, TIMESTAMP '2025-01-06 08:05:00', TIMESTAMP '2025-01-06 08:00:00', 22.0, 900.0, 4.0, 0, 0),
                (46, 50, TIMESTAMP '2025-01-06 08:25:00', TIMESTAMP '2025-01-06 08:20:00', 18.0, 840.0, 3.5, 0, 0)
        ) AS t(PULocationID, DOLocationID, pickup_datetime, request_datetime, driver_pay, trip_time, trip_miles, shared_match_flag, shared_request_flag)
        """
    )
    con.execute(f"COPY trips TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_all_blend_and_damp_columns_exist_in_engine_output(tmp_path: Path) -> None:
    parquet_path = tmp_path / "tiny.parquet"
    _write_tiny_parquet(parquet_path)

    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TEMP TABLE zone_geometry_metrics (PULocationID INTEGER, zone_area_sq_miles DOUBLE, centroid_latitude DOUBLE)")
    con.execute("INSERT INTO zone_geometry_metrics VALUES (46, 0.25, 40.845)")
    con.execute("CREATE TEMP TABLE zone_metadata (PULocationID INTEGER, zone_name VARCHAR, borough_name VARCHAR, airport_excluded BOOLEAN)")
    con.execute("INSERT INTO zone_metadata VALUES (46, 'City Island', 'Bronx', FALSE)")
    con.execute("INSERT INTO zone_metadata VALUES (50, 'Reference', 'Bronx', FALSE)")

    sql = build_zone_earnings_shadow_sql([str(parquet_path)], bin_minutes=20, min_trips_per_window=1, **_shadow_profile_kwargs())
    cur = con.execute(sql)
    out_cols = {str(desc[0]) for desc in cur.description}
    con.close()

    needed = set(SAME_WEEKDAY_BLEND_COLUMNS) | set(SAME_WEEKDAY_DAMP_TARGET_COLUMNS) | {"pickups_now"}
    missing = sorted(needed - out_cols)
    assert not missing, f"blend/damp columns missing from engine output: {missing}"


# --------------------------------------------------------------------------- #
# End-to-end: a spiky zone is rated below a steady one through the real build
# --------------------------------------------------------------------------- #
def _write_three_zone_geojson(path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"LocationID": 46, "zone": "City Island", "borough": "Bronx"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-73.79, 40.84], [-73.78, 40.84], [-73.78, 40.85], [-73.79, 40.85], [-73.79, 40.84]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"LocationID": 50, "zone": "Reference", "borough": "Bronx"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-73.88, 40.83], [-73.87, 40.83], [-73.87, 40.84], [-73.88, 40.84], [-73.88, 40.83]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"LocationID": 132, "zone": "JFK Airport", "borough": "Queens"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-73.9, 40.6], [-73.85, 40.6], [-73.85, 40.65], [-73.9, 40.65], [-73.9, 40.6]]],
                },
            },
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


# today + the three exact prior same-weekday instants (whole-week offsets keep
# the local wall clock and weekday identical -- January has no DST change).
_E2E_TIMES = {
    "today": "2025-01-29 18:05:00",
    "w1": "2025-01-22 18:05:00",
    "w2": "2025-01-15 18:05:00",
    "w3": "2025-01-08 18:05:00",
}


def _write_multiweek_parquet(path: Path, *, zone46_prior_count: int, per_bin: int = 10) -> None:
    rows: list[tuple] = []

    def add(zone: int, do: int, ts: str, n: int) -> None:
        for _ in range(n):
            rows.append((zone, do, ts, ts, 20.0, 900.0, 4.0, 0, 0))

    # Zone 46: always busy today; busy in the prior weeks only when asked.
    add(46, 50, _E2E_TIMES["today"], per_bin)
    for key in ("w1", "w2", "w3"):
        add(46, 50, _E2E_TIMES[key], zone46_prior_count)
    # Reference zone 50 is identical in both parquets and present in ALL four
    # bins, so the prior bins exist (zone 46 then appears with pickups_now=0
    # there even when it has no trips).
    for key in _E2E_TIMES:
        add(50, 46, _E2E_TIMES[key], per_bin)

    con = duckdb.connect(database=":memory:")
    con.execute(
        "CREATE TABLE trips (PULocationID INTEGER, DOLocationID INTEGER, pickup_datetime TIMESTAMP, "
        "request_datetime TIMESTAMP, driver_pay DOUBLE, trip_time DOUBLE, trip_miles DOUBLE, "
        "shared_match_flag INTEGER, shared_request_flag INTEGER)"
    )
    con.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY trips TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _discover_zone_today_bin(parquet_path: Path, geojson_path: Path, location_id: int, index: int = -1) -> str:
    """Run the engine SQL (no window) and return one of a zone's bin keys
    (default the latest; pass index=-2 for the penultimate, i.e. a bin that
    still has a "next bin" after it).

    Done under the same DuckDB defaults build_single_frame_for_month uses, so
    the discovered bin matches what the serve path will look back from.
    """
    from zone_geometry_metrics import build_zone_geometry_metrics_rows

    zones = json.loads(geojson_path.read_text(encoding="utf-8"))
    name_by_id, borough_by_id = {}, {}
    for feature in zones["features"]:
        props = feature["properties"]
        zid = int(props["LocationID"])
        name_by_id[zid] = props.get("zone", "")
        borough_by_id[zid] = props.get("borough", "")
    geom_rows = build_zone_geometry_metrics_rows(geojson_path)
    meta_rows = [
        (zid, name_by_id[zid], borough_by_id[zid], bool(bh.is_airport_zone(zid, name_by_id[zid], borough_by_id[zid])))
        for zid in sorted(name_by_id)
    ]
    con = bh._open_shadow_sql_connection(
        database_path=":memory:", zone_geometry_rows=geom_rows, zone_metadata_rows=meta_rows
    )
    try:
        sql = build_zone_earnings_shadow_sql([str(parquet_path)], bin_minutes=20, min_trips_per_window=1, **_shadow_profile_kwargs())
        cur = con.execute(
            f"WITH r AS ({sql}) SELECT DISTINCT strftime(exact_bin_local_ts AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S') AS k "
            f"FROM r WHERE PULocationID = ? ORDER BY k",
            [location_id],
        )
        bins = [str(row[0]) for row in cur.fetchall()]
    finally:
        con.close()
    assert bins, "expected at least one bin for the zone"
    return bins[index]


def _citywide_rating_for_zone(frame: dict, location_id: int) -> int:
    for feature in frame["polygons"]["features"]:
        props = feature["properties"]
        if int(props["LocationID"]) == location_id:
            return int(props["earnings_shadow_rating_citywide_v3"])
    raise AssertionError(f"zone {location_id} not in frame")


def test_spiky_zone_rated_below_steady_zone_end_to_end(tmp_path: Path) -> None:
    geojson_path = tmp_path / "taxi_zones.geojson"
    _write_three_zone_geojson(geojson_path)
    steady_parquet = tmp_path / "steady.parquet"
    spiky_parquet = tmp_path / "spiky.parquet"
    _write_multiweek_parquet(steady_parquet, zone46_prior_count=10)  # busy every week
    _write_multiweek_parquet(spiky_parquet, zone46_prior_count=0)    # busy only today

    frame_time = _discover_zone_today_bin(steady_parquet, geojson_path, 46)

    steady_frame = bh.build_single_frame_for_month(
        parquet_files=[steady_parquet], zones_geojson_path=geojson_path,
        frame_time=frame_time, bin_minutes=20, min_trips_per_window=1,
    )
    spiky_frame = bh.build_single_frame_for_month(
        parquet_files=[spiky_parquet], zones_geojson_path=geojson_path,
        frame_time=frame_time, bin_minutes=20, min_trips_per_window=1,
    )

    steady_rating = _citywide_rating_for_zone(steady_frame, 46)
    spiky_rating = _citywide_rating_for_zone(spiky_frame, 46)

    # Same today data in both; the only difference is that the spiky zone is
    # dead in the prior weeks -> blended down AND damped -> strictly lower.
    assert spiky_rating < steady_rating

    # Colors stay well-formed for every zone in both frames.
    for frame in (steady_frame, spiky_frame):
        for feature in frame["polygons"]["features"]:
            props = feature["properties"]
            if int(props["LocationID"]) == 132:
                continue  # airport zone is excluded from coloring
            assert props.get("earnings_shadow_color_citywide_v3") is not None
            assert math.isfinite(float(props["earnings_shadow_visible_score_citywide_v3"]))


def test_store_path_blends_after_tz_normalization(tmp_path: Path) -> None:
    """Regression guard for the store-path TIMESTAMPTZ no-op: exact_bin_local_ts
    is a TIMESTAMPTZ whose raw str() is "... 18:00:00+00:00" and never equals
    the T-formatted requested_frame_time. Without the `AT TIME ZONE 'UTC'`
    bin-key normalization the store path fails to find the "today" sample and
    silently skips the blend (returning {}), which would then diverge from the
    working parquet serve path. This builds an exact_shadow_rows table exactly
    like build_hotspots_frames (CREATE TABLE AS SELECT, same TIMESTAMPTZ type)
    and asserts the store path identifies today and actually moves the value.
    """
    from zone_geometry_metrics import build_zone_geometry_metrics_rows

    geojson = tmp_path / "z.geojson"
    _write_three_zone_geojson(geojson)
    parquet = tmp_path / "p.parquet"
    _write_multiweek_parquet(parquet, zone46_prior_count=3)  # priors differ from today
    req = _discover_zone_today_bin(parquet, geojson, 46)

    zones = json.loads(geojson.read_text(encoding="utf-8"))
    name_by_id = {int(f["properties"]["LocationID"]): f["properties"].get("zone", "") for f in zones["features"]}
    borough_by_id = {int(f["properties"]["LocationID"]): f["properties"].get("borough", "") for f in zones["features"]}
    geom_rows = build_zone_geometry_metrics_rows(geojson)
    meta_rows = [
        (zid, name_by_id[zid], borough_by_id[zid], bool(bh.is_airport_zone(zid, name_by_id[zid], borough_by_id[zid])))
        for zid in sorted(name_by_id)
    ]
    anchor_col = "earnings_shadow_score_citywide_v3_anchor_shadow"
    con = bh._open_shadow_sql_connection(database_path=":memory:", zone_geometry_rows=geom_rows, zone_metadata_rows=meta_rows)
    try:
        sql = build_zone_earnings_shadow_sql(
            [str(parquet)], bin_minutes=20, min_trips_per_window=1,
            pickup_utc_start="2025-01-01T00:00:00Z", pickup_utc_end="2025-02-01T00:00:00Z",
            **_shadow_profile_kwargs(),
        )
        con.execute(f"CREATE TABLE exact_shadow_rows AS SELECT * FROM ({sql})")
        blended = bh._same_weekday_blended_scores(con, req, SAME_WEEKDAY_BLEND_COLUMNS)
        raw_today = con.execute(
            f"SELECT {anchor_col} FROM exact_shadow_rows WHERE PULocationID = 46 "
            f"AND strftime(exact_bin_local_ts AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S') = ?",
            [req],
        ).fetchone()
    finally:
        con.close()

    assert 46 in blended, "store path failed to identify the 'today' sample (blend no-op)"
    assert anchor_col in blended[46]
    # The blend mixes in the (different) prior weeks and damps, so the stored
    # value is genuinely moved off raw today -- proving it is not a no-op.
    assert raw_today is not None
    assert blended[46][anchor_col] != pytest.approx(float(raw_today[0]))


# today + 3 prior weeks, each with a CURRENT bin (18:05 -> bin :00) and a NEXT
# bin 20 min later (18:25 -> bin :20), so the served frame can look one bin ahead.
_TREND_TIMES = {
    "today": ("2025-01-29 18:05:00", "2025-01-29 18:25:00"),
    "w1": ("2025-01-22 18:05:00", "2025-01-22 18:25:00"),
    "w2": ("2025-01-15 18:05:00", "2025-01-15 18:25:00"),
    "w3": ("2025-01-08 18:05:00", "2025-01-08 18:25:00"),
}


def _write_trend_parquet(path: Path, *, z46_now: int, z46_next: int, ref: int = 10) -> None:
    rows: list[tuple] = []

    def add(zone: int, do: int, ts: str, n: int) -> None:
        for _ in range(n):
            rows.append((zone, do, ts, ts, 20.0, 900.0, 4.0, 0, 0))

    for cur_ts, next_ts in _TREND_TIMES.values():
        add(46, 50, cur_ts, z46_now)   # zone 46: this bin
        add(46, 50, next_ts, z46_next)  # zone 46: next bin
        add(50, 46, cur_ts, ref)        # reference zone keeps both bins alive
        add(50, 46, next_ts, ref)

    con = duckdb.connect(database=":memory:")
    con.execute(
        "CREATE TABLE trips (PULocationID INTEGER, DOLocationID INTEGER, pickup_datetime TIMESTAMP, "
        "request_datetime TIMESTAMP, driver_pay DOUBLE, trip_time DOUBLE, trip_miles DOUBLE, "
        "shared_match_flag INTEGER, shared_request_flag INTEGER)"
    )
    con.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY trips TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_next_bin_trend_surfaces_cooling_zone(tmp_path: Path) -> None:
    """A zone that is busy now but quiet in the next 20-min bin gets a lower
    `..._next` rating + the frame carries `next_time`, so the map can warn the
    driver it is about to cool. The steady reference zone barely moves."""
    geojson = tmp_path / "z.geojson"
    _write_three_zone_geojson(geojson)
    parquet = tmp_path / "trend.parquet"
    _write_trend_parquet(parquet, z46_now=15, z46_next=1)
    # Build at the CURRENT bin (penultimate), so the +1 bin exists.
    frame_time = _discover_zone_today_bin(parquet, geojson, 46, index=-2)

    frame = bh.build_single_frame_for_month(
        parquet_files=[parquet], zones_geojson_path=geojson, frame_time=frame_time,
        bin_minutes=20, min_trips_per_window=1,
    )

    assert frame.get("next_time"), "frame should carry the next bin's time"
    props = {int(f["properties"]["LocationID"]): f["properties"] for f in frame["polygons"]["features"]}

    cur46 = props[46]["earnings_shadow_rating_citywide_v3"]
    next46 = props[46].get("earnings_shadow_rating_citywide_v3_next")
    assert next46 is not None, "cooling zone must carry a next-bin rating"
    assert next46 < cur46, "zone 46 is quiet next bin -> next rating must be lower"

    # Steady reference zone: next ~ current (no big swing).
    cur50 = props[50]["earnings_shadow_rating_citywide_v3"]
    next50 = props[50].get("earnings_shadow_rating_citywide_v3_next")
    assert next50 is not None
    assert abs(next50 - cur50) <= 20
