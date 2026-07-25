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


def test_next_rating_moves_with_its_base_rating():
    """The "_next" companion drives the map's trend label. Scaling the now-value
    while leaving next alone would invent an 'about to heat up' on every capped
    zone in a quiet frame."""
    feats = _frame(3000, [90, 70])
    for f, nxt in zip(feats, [88, 69]):
        f["properties"]["earnings_shadow_rating_citywide_v3_next"] = nxt
    apply_frame_demand_ceiling(feats, _bps())
    p = feats[0]["properties"]
    now = p["earnings_shadow_rating_citywide_v3"]
    nxt = p["earnings_shadow_rating_citywide_v3_next"]
    assert nxt < 88, "next must be scaled too"
    # Before: 90 -> 88 is a mild fade. After scaling both, it must STILL read as
    # a mild fade, not a climb.
    assert nxt <= now + 1, f"capping invented a fake climb: now={now} next={nxt}"


def test_per_zone_anchor_also_keeps_next_in_step():
    from build_hotspot import _apply_same_ratio_to_next
    props = {"r": 90, "r_next": 88}
    _apply_same_ratio_to_next(props, "r", 90, 72)
    assert props["r_next"] == 70, props["r_next"]


def test_same_ratio_helper_never_raises_next():
    from build_hotspot import _apply_same_ratio_to_next
    props = {"r": 50, "r_next": 60}
    _apply_same_ratio_to_next(props, "r", 50, 50)   # unchanged base
    assert props["r_next"] == 60
    _apply_same_ratio_to_next(props, "r", 50, 80)   # base raised (shouldn't happen)
    assert props["r_next"] == 60


# --- benchmark basis ------------------------------------------------------
# The distribution must be built on the same basis as the value scored against
# it. The store holds EXACT per-bin counts; the frames we serve carry a
# same-weekday BLEND. Blending removes variance, so ranking a blended value
# against an exact distribution squeezes both ends of the scale.

def _seeded_store(path):
    import datetime
    import random
    import duckdb
    random.seed(7)
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE exact_shadow_rows "
        "(PULocationID INTEGER, exact_bin_local_ts TIMESTAMP, pickups_now INTEGER)"
    )
    rows = []
    base = datetime.datetime(2025, 10, 1)
    for d in range(28):
        day_mult = random.uniform(0.75, 1.25)   # real day-to-day variance
        for b in range(72):
            hour = b * 20 / 60.0
            diurnal = 0.15 + 0.85 * max(0.0, math.sin((hour - 4) / 24 * 2 * math.pi) + 0.35)
            total = 3000 * diurnal * day_mult
            ts = base + datetime.timedelta(days=d, minutes=20 * b)
            for z in range(5):
                rows.append((z + 1, ts, int(max(0, random.gauss(total / 5, total / 25)))))
    con.executemany("INSERT INTO exact_shadow_rows VALUES (?,?,?)", rows)
    return con


def test_frame_breakpoints_use_the_blended_basis(tmp_path):
    from build_hotspot import (
        _MONTH_DEMAND_BP_LAST_ERROR,
        _compute_frame_demand_breakpoints_from_store,
    )
    _MONTH_DEMAND_BP_LAST_ERROR.pop("frame_blended", None)
    con = _seeded_store(tmp_path / "s.duckdb")
    try:
        bps = _compute_frame_demand_breakpoints_from_store(con)
    finally:
        con.close()
    assert len(bps) == 101
    # The blended query must actually have run, not silently fallen back.
    assert _MONTH_DEMAND_BP_LAST_ERROR.get("frame_blended") is None


def test_blended_basis_stops_over_capping_quiet_frames(tmp_path):
    """The defect: on an exact-count distribution a blended trough looked far
    more extreme than it is, so quiet frames were capped harder than warranted."""
    from month_color_benchmark import score_on_breakpoints
    from build_hotspot import _compute_frame_demand_breakpoints_from_store

    con = _seeded_store(tmp_path / "s2.duckdb")
    try:
        blended_bps = _compute_frame_demand_breakpoints_from_store(con)
        qlist = "[" + ", ".join(f"{i/100.0:.4f}" for i in range(101)) + "]"
        exact_bps = sorted(float(x) for x in con.execute(
            f"SELECT QUANTILE_CONT(LN(1+t),{qlist}) FROM ("
            f"SELECT exact_bin_local_ts, SUM(pickups_now) t FROM exact_shadow_rows GROUP BY 1)"
        ).fetchone()[0])
        trough = min(r[0] for r in con.execute(
            "SELECT AVG(t) FROM (SELECT exact_bin_local_ts ts, SUM(pickups_now) t "
            "FROM exact_shadow_rows GROUP BY 1) "
            "GROUP BY EXTRACT(DOW FROM (ts AT TIME ZONE 'UTC')), "
            "EXTRACT(HOUR FROM (ts AT TIME ZONE 'UTC'))*60"
            "+EXTRACT(MINUTE FROM (ts AT TIME ZONE 'UTC'))"
        ).fetchall())
    finally:
        con.close()

    on_blended = score_on_breakpoints(trough, blended_bps)
    on_exact = score_on_breakpoints(trough, exact_bps)
    assert on_blended > on_exact + 5, (
        f"blended basis should place a blended trough meaningfully higher than the "
        f"exact basis did (blended={on_blended:.1f}% exact={on_exact:.1f}%)"
    )
