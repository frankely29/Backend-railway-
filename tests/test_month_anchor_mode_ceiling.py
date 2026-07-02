"""Borough-mode color tracks get the same monthly-benchmark CEILING as citywide.

Each mode's rating had the same per-frame flaw (best zone ~green even on a quiet
night). The month benchmark caps each mode by the month-wide distribution of that
mode's OWN earnings score, using that mode's blend -- so it can only pull unearned
greens down, never raise a rating, and only differs from the per-frame rating in
that the rank + citywide anchor terms become month-anchored.
"""
from __future__ import annotations

import math

from build_hotspot import (
    _apply_month_anchored_colors,
    MONTH_BENCHMARK_FEATURE_FIELD,
    MONTH_BENCHMARK_CONF_FIELD,
)


def _month_breakpoints():
    vals = sorted(math.log1p(x / 1000.0) for x in range(0, 1001))
    n = len(vals)
    return [vals[min(n - 1, round(i / 100 * (n - 1)))] for i in range(101)]


def _feature(zid, cw_score, cw_rating, m_score, m_rating):
    return {
        "properties": {
            "LocationID": zid,
            "airport_excluded": False,
            MONTH_BENCHMARK_FEATURE_FIELD: cw_score,
            MONTH_BENCHMARK_CONF_FIELD: 0.5,
            "earnings_shadow_rating_citywide_v3": cw_rating,
            "earnings_shadow_score_raw_manhattan_v3": m_score,
            "earnings_shadow_confidence_manhattan_v3": 0.5,
            "earnings_shadow_rating_manhattan_v3": m_rating,
            "earnings_shadow_bucket_manhattan_v3": "green",
            "earnings_shadow_color_manhattan_v3": "#00b050",
        }
    }


def test_mode_ceiling_caps_quiet_and_never_raises():
    bps = _month_breakpoints()
    mode_bps = {"manhattan_v3": bps}
    quiet = _feature(10, 0.02, 95, 0.02, 95)  # low absolute mode score, high per-frame rating
    busy = _feature(11, 0.9, 90, 0.95, 88)    # high absolute mode score
    _apply_month_anchored_colors([quiet, busy], bps, mode_bps)
    # Quiet-mode zone loses its unearned green.
    assert quiet["properties"]["earnings_shadow_rating_manhattan_v3"] < 83
    assert quiet["properties"]["earnings_shadow_bucket_manhattan_v3"] != "green"
    # Ceiling never raises a rating.
    assert busy["properties"]["earnings_shadow_rating_manhattan_v3"] <= 88


def test_mode_ceiling_is_noop_without_mode_breakpoints():
    bps = _month_breakpoints()
    f = _feature(10, 0.9, 90, 0.9, 87)
    _apply_month_anchored_colors([f], bps, None)
    # Mode rating untouched when no mode breakpoints are supplied.
    assert f["properties"]["earnings_shadow_rating_manhattan_v3"] == 87


def test_mode_missing_score_or_rating_left_untouched():
    bps = _month_breakpoints()
    f = _feature(10, 0.9, 90, 0.9, 87)
    del f["properties"]["earnings_shadow_score_raw_manhattan_v3"]
    _apply_month_anchored_colors([f], bps, {"manhattan_v3": bps})
    assert f["properties"]["earnings_shadow_rating_manhattan_v3"] == 87
