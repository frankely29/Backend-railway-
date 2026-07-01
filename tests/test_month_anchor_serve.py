"""Serve-time month-anchored color remap.

The served frame's visible citywide rating is set by a PER-FRAME rank in
_recalibrate_visible_v3_fields, which is why every frame has greens. This tests
the override that re-bases each zone's color to its month-wide percentile of the
composite EARNINGS score (saturation + all formulas included) so the same
earnings quality -> same color every day, airports untouched.
"""
from __future__ import annotations

import math

from build_hotspot import _apply_month_anchored_colors, MONTH_BENCHMARK_FEATURE_FIELD


def _month_breakpoints():
    # A month whose zone-frame earnings scores span 0..1; 101 evenly-spaced
    # quantiles (built with the same LN(1+x) transform the SQL benchmark uses).
    vals = sorted(math.log1p(x / 1000.0) for x in range(0, 1001))
    n = len(vals)
    return [vals[min(n - 1, round(i / 100 * (n - 1)))] for i in range(101)]


def _feature(zone_id, earnings_score, airport=False):
    return {
        "properties": {
            "LocationID": zone_id,
            MONTH_BENCHMARK_FEATURE_FIELD: earnings_score,
            "airport_excluded": airport,
            "earnings_shadow_rating_citywide_v3": 90,   # seeded "green" per-frame rank
            "earnings_shadow_bucket_citywide_v3": "green",
            "earnings_shadow_color_citywide_v3": "#00b050",
        }
    }


def test_low_quality_zone_drops_out_of_green_high_quality_stays_airport_untouched():
    bps = _month_breakpoints()
    low = _feature(10, 0.02)       # bottom of the month's earnings distribution
    high = _feature(11, 0.97)      # near the top
    airport = _feature(132, 0.99, airport=True)
    feats = [low, high, airport]
    _apply_month_anchored_colors(feats, bps)

    assert low["properties"]["earnings_shadow_bucket_citywide_v3"] != "green"
    assert low["properties"]["earnings_shadow_rating_citywide_v3"] < 40
    assert high["properties"]["earnings_shadow_bucket_citywide_v3"] == "green"
    # airport left exactly as seeded
    assert airport["properties"]["earnings_shadow_rating_citywide_v3"] == 90
    assert airport["properties"]["earnings_shadow_bucket_citywide_v3"] == "green"


def test_same_score_same_color_across_calls():
    bps = _month_breakpoints()
    a = _feature(10, 0.4)
    b = _feature(20, 0.4)
    _apply_month_anchored_colors([a], bps)
    _apply_month_anchored_colors([b], bps)
    assert a["properties"]["earnings_shadow_rating_citywide_v3"] == b["properties"]["earnings_shadow_rating_citywide_v3"]


def test_order_preserved():
    bps = _month_breakpoints()
    feats = [_feature(i, sc) for i, sc in enumerate([0.05, 0.2, 0.4, 0.6, 0.95])]
    _apply_month_anchored_colors(feats, bps)
    ratings = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats]
    assert ratings == sorted(ratings)


def test_missing_score_is_left_untouched():
    f = _feature(10, 0.02)
    del f["properties"][MONTH_BENCHMARK_FEATURE_FIELD]
    _apply_month_anchored_colors([f], _month_breakpoints())
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] == 90


def test_color_follows_earnings_quality_not_raw_pickups():
    # The regression this fixes: a high-pickup but LOW-earnings-quality zone must
    # NOT outrank a lower-pickup but HIGHER-quality zone. The benchmark ranks the
    # earnings score, so the higher-quality zone rates at least as high regardless
    # of pickup volume. pickups_now_shadow is deliberately present and inverted to
    # prove it is not what drives the color.
    bps = _month_breakpoints()
    high_quality_low_pickups = _feature(9, 0.356)
    high_quality_low_pickups["properties"]["pickups_now_shadow"] = 10
    low_quality_high_pickups = _feature(4, 0.341)
    low_quality_high_pickups["properties"]["pickups_now_shadow"] = 57
    _apply_month_anchored_colors([high_quality_low_pickups, low_quality_high_pickups], bps)
    assert (
        high_quality_low_pickups["properties"]["earnings_shadow_rating_citywide_v3"]
        >= low_quality_high_pickups["properties"]["earnings_shadow_rating_citywide_v3"]
    )


def test_no_breakpoints_is_a_noop():
    f = _feature(10, 0.02)
    _apply_month_anchored_colors([f], [])
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] == 90
