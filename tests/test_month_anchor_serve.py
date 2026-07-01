"""Serve-time month-anchored color remap.

The served frame's visible citywide rating is set by a PER-FRAME rank in
_recalibrate_visible_v3_fields, which is why every frame has greens. This tests
the override that re-bases each zone's color to its month-wide demand percentile
so the same demand -> same color every day, airports untouched.
"""
from __future__ import annotations

import math

from build_hotspot import _apply_month_anchored_colors


def _month_breakpoints():
    # A month whose zone-frame pickups span 0..300; 101 evenly-spaced quantiles.
    vals = sorted(math.log1p(x) for x in range(0, 301))
    n = len(vals)
    return [vals[min(n - 1, round(i / 100 * (n - 1)))] for i in range(101)]


def _feature(zone_id, pickups, airport=False):
    return {
        "properties": {
            "LocationID": zone_id,
            "pickups_now_shadow": pickups,
            "airport_excluded": airport,
            "earnings_shadow_rating_citywide_v3": 90,   # seeded "green" per-frame rank
            "earnings_shadow_bucket_citywide_v3": "green",
            "earnings_shadow_color_citywide_v3": "#00b050",
        }
    }


def test_quiet_zone_drops_out_of_green_busy_stays_airport_untouched():
    bps = _month_breakpoints()
    quiet = _feature(10, 2)       # 2 pickups -> bottom of the month
    busy = _feature(11, 295)      # near the top
    airport = _feature(132, 500, airport=True)
    feats = [quiet, busy, airport]
    _apply_month_anchored_colors(feats, bps)

    assert quiet["properties"]["earnings_shadow_bucket_citywide_v3"] != "green"
    assert quiet["properties"]["earnings_shadow_rating_citywide_v3"] < 40
    assert busy["properties"]["earnings_shadow_bucket_citywide_v3"] == "green"
    # airport left exactly as seeded
    assert airport["properties"]["earnings_shadow_rating_citywide_v3"] == 90
    assert airport["properties"]["earnings_shadow_bucket_citywide_v3"] == "green"


def test_same_pickups_same_color_across_calls():
    bps = _month_breakpoints()
    a = _feature(10, 120)
    b = _feature(20, 120)
    _apply_month_anchored_colors([a], bps)
    _apply_month_anchored_colors([b], bps)
    assert a["properties"]["earnings_shadow_rating_citywide_v3"] == b["properties"]["earnings_shadow_rating_citywide_v3"]


def test_order_preserved():
    bps = _month_breakpoints()
    feats = [_feature(i, pk) for i, pk in enumerate([5, 40, 100, 180, 290])]
    _apply_month_anchored_colors(feats, bps)
    ratings = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats]
    assert ratings == sorted(ratings)


def test_no_breakpoints_is_a_noop():
    f = _feature(10, 2)
    _apply_month_anchored_colors([f], [])
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] == 90
