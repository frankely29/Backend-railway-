"""Frame-level demand ceiling.

The per-zone month anchor cannot cap a quiet hour: the score it ranks nets
saturation OUT of demand, so 2am (few riders, few drivers) lands at almost
exactly the midday score. Production July 2025 showed Fri 02:00 (5,360 pickups)
producing MORE greens than Fri 12:00 (11,182 pickups).

This ceiling ranks the frame's citywide pickup total against the month and caps
what the frame's BEST zone may read -- "if the best it got was 75, it doesn't
qualify for green" -- while SCALING (not clamping) so ordering survives.
"""
from __future__ import annotations

import math

from build_hotspot import (
    FRAME_DEMAND_CEILING_FLOOR,
    apply_frame_demand_ceiling,
    frame_demand_ceiling_rating,
)


def _bps():
    """Month frame-demand curve spanning 2,000 -> 16,000 citywide pickups."""
    return sorted(math.log1p(2000 + (14000 * i / 100.0)) for i in range(101))


def _frame(pickups_total, ratings, field="earnings_shadow_rating_citywide_v3"):
    n = len(ratings)
    per = pickups_total / float(n)
    feats = []
    for i, r in enumerate(ratings):
        feats.append({
            "properties": {
                "LocationID": i + 1,
                "pickups_now": per,
                field: r,
                field.replace("_rating_", "_bucket_"): "green",
                field.replace("_rating_", "_color_"): "#00b050",
            }
        })
    return feats


def test_quiet_frame_is_capped_below_green():
    feats = _frame(3000, [90, 84, 70, 50, 30])
    ceiling = apply_frame_demand_ceiling(feats, _bps())
    assert ceiling is not None
    top = feats[0]["properties"]["earnings_shadow_rating_citywide_v3"]
    assert top < 83, f"quiet frame still reads green: {top}"
    assert feats[0]["properties"]["earnings_shadow_bucket_citywide_v3"] != "green"


def test_busy_frame_is_untouched():
    ratings = [90, 84, 70, 50, 30]
    feats = _frame(15500, list(ratings))
    apply_frame_demand_ceiling(feats, _bps())
    got = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats]
    assert got == ratings, "a peak frame must keep its normal colors"


def test_ceiling_never_raises_a_rating():
    feats = _frame(15500, [40, 30, 20])
    apply_frame_demand_ceiling(feats, _bps())
    got = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats]
    assert got == [40, 30, 20], "min-semantics: a weak frame is not inflated"


def test_ordering_and_spread_survive_the_cap():
    feats = _frame(3000, [90, 84, 70, 50, 30])
    apply_frame_demand_ceiling(feats, _bps())
    got = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in feats]
    assert got == sorted(got, reverse=True), "ranking must survive"
    assert len(set(got)) == len(got), "zones must stay distinguishable (scale, not clamp)"


def test_quietest_frame_still_leaves_a_workable_floor():
    feats = _frame(100, [90, 60, 30])
    ceiling = apply_frame_demand_ceiling(feats, _bps())
    assert ceiling >= FRAME_DEMAND_CEILING_FLOOR
    top = feats[0]["properties"]["earnings_shadow_rating_citywide_v3"]
    assert top >= 50, "night drivers still need a usable best-zone read"


def test_quiet_beats_busy_is_corrected():
    """The exact production inversion: quiet frame must not out-green a busy one."""
    quiet = _frame(5360, [90, 85, 84, 70])
    busy = _frame(11182, [90, 85, 84, 70])
    apply_frame_demand_ceiling(quiet, _bps())
    apply_frame_demand_ceiling(busy, _bps())
    q = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in quiet]
    b = [f["properties"]["earnings_shadow_rating_citywide_v3"] for f in busy]
    q_greens = sum(1 for x in q if x >= 83)
    b_greens = sum(1 for x in b if x >= 83)
    assert q_greens <= b_greens, f"quiet {q} still out-greens busy {b}"


def test_all_color_tracks_are_capped_including_45plus():
    feats = _frame(3000, [90, 70], field="earnings_shadow_rating_citywide_v3")
    for f, r in zip(feats, [95, 60]):
        f["properties"]["earnings_shadow_rating_trips_45plus_v3"] = r
        f["properties"]["earnings_shadow_bucket_trips_45plus_v3"] = "green"
    apply_frame_demand_ceiling(feats, _bps())
    assert feats[0]["properties"]["earnings_shadow_rating_trips_45plus_v3"] < 83, \
        "the 45+ track must be day-consistent too"


def test_no_breakpoints_is_a_noop():
    feats = _frame(3000, [90, 70])
    assert apply_frame_demand_ceiling(feats, []) is None
    assert feats[0]["properties"]["earnings_shadow_rating_citywide_v3"] == 90


def test_missing_pickups_is_a_noop():
    feats = _frame(3000, [90, 70])
    for f in feats:
        f["properties"].pop("pickups_now")
    assert frame_demand_ceiling_rating(feats, _bps()) is None


def test_airport_zones_excluded_from_frame_total():
    feats = _frame(3000, [90, 70])
    feats.append({"properties": {"LocationID": 132, "pickups_now": 999999,
                                 "airport_excluded": True,
                                 "earnings_shadow_rating_citywide_v3": 95}})
    ceiling = frame_demand_ceiling_rating(feats, _bps())
    plain = frame_demand_ceiling_rating(_frame(3000, [90, 70]), _bps())
    assert ceiling == plain, "an airport must not inflate the frame's demand"
