"""Serve-time monthly benchmark applied as a CEILING on the per-frame rating.

The per-frame recalibration already ranks zones best-to-worst with all the
earnings formulas; its only flaw is re-normalizing inside each 20-min frame, so
the best zone is ~green even on a dead-quiet night. The monthly benchmark fixes
ONLY that by capping the rating at what the zone's absolute month level earns:
final = min(per_frame_rating, month_ceiling). It can never RAISE a rating (so no
extra greens ever appear), only pull down unearned ones on quiet frames.
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


def _feature(zone_id, earnings_score, per_frame_rating=90, conf=0.0, airport=False):
    return {
        "properties": {
            "LocationID": zone_id,
            MONTH_BENCHMARK_FEATURE_FIELD: earnings_score,
            "earnings_shadow_confidence_citywide_v3": conf,
            "airport_excluded": airport,
            "earnings_shadow_rating_citywide_v3": per_frame_rating,
            "earnings_shadow_bucket_citywide_v3": "green",
            "earnings_shadow_color_citywide_v3": "#00b050",
        }
    }


def test_ceiling_never_raises_a_rating():
    # A modest per-frame rating with a high month level must NOT be promoted.
    bps = _month_breakpoints()
    f = _feature(10, 0.98, per_frame_rating=45)  # ceiling would be high
    _apply_month_anchored_colors([f], bps)
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] <= 45


def test_ceiling_caps_unearned_green_on_quiet_frame():
    # Per-frame rank says green (95), but the zone's absolute month level is tiny,
    # so the ceiling pulls it out of green -- "the best only got 75, no green".
    bps = _month_breakpoints()
    f = _feature(10, 0.02, per_frame_rating=95)
    _apply_month_anchored_colors([f], bps)
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] < 83
    assert f["properties"]["earnings_shadow_bucket_citywide_v3"] != "green"


def test_busy_frame_keeps_per_frame_colors():
    # High month level AND solid confidence -> high ceiling -> the per-frame rating
    # is preserved, so a busy frame looks exactly like it always did (no extra
    # greens, none removed).
    bps = _month_breakpoints()
    feats = [_feature(i, 0.95, per_frame_rating=r, conf=0.9) for i, r in enumerate([30, 55, 70, 88])]
    _apply_month_anchored_colors(feats, bps)
    assert [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats] == [30, 55, 70, 88]


def test_airport_untouched():
    bps = _month_breakpoints()
    airport = _feature(132, 0.02, per_frame_rating=90, airport=True)
    _apply_month_anchored_colors([airport], bps)
    assert airport["properties"]["earnings_shadow_rating_citywide_v3"] == 90
    assert airport["properties"]["earnings_shadow_bucket_citywide_v3"] == "green"


def test_higher_month_level_never_caps_below_lower_one():
    # Two zones with equal per-frame rating: the higher-earnings-quality zone must
    # not end up rated below the lower-quality one after capping.
    bps = _month_breakpoints()
    high = _feature(9, 0.60, per_frame_rating=90)
    low = _feature(4, 0.30, per_frame_rating=90)
    _apply_month_anchored_colors([high, low], bps)
    assert (
        high["properties"]["earnings_shadow_rating_citywide_v3"]
        >= low["properties"]["earnings_shadow_rating_citywide_v3"]
    )


def test_missing_score_or_rating_is_left_untouched():
    bps = _month_breakpoints()
    no_score = _feature(10, 0.02)
    del no_score["properties"][MONTH_BENCHMARK_FEATURE_FIELD]
    _apply_month_anchored_colors([no_score], bps)
    assert no_score["properties"]["earnings_shadow_rating_citywide_v3"] == 90


def test_no_breakpoints_is_a_noop():
    f = _feature(10, 0.02, per_frame_rating=90)
    _apply_month_anchored_colors([f], [])
    assert f["properties"]["earnings_shadow_rating_citywide_v3"] == 90
