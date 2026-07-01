"""The month-anchored color scale makes a color mean the same busy-ness daily."""
from __future__ import annotations

from month_color_benchmark import (
    build_month_demand_scale,
    score_on_scale,
    bucket_for_score,
    zone_month_average_score,
    tendency_from_benchmark,
)


def _month_scale():
    # A month whose zone-frame pickups span 0..200; green (>=83) needs the top
    # ~17% of the month's demand.
    return build_month_demand_scale([float(p) for p in range(0, 201)] * 3)


def test_same_raw_demand_scores_the_same_on_any_day():
    scale = _month_scale()
    # The scale is the whole month, so a given pickup count maps to ONE score
    # regardless of what other zones are doing that day — colors are consistent.
    assert score_on_scale(150, scale) == score_on_scale(150, scale)
    assert score_on_scale(150, scale) > score_on_scale(60, scale)


def test_quiet_day_does_not_reach_green():
    # July-5 example: on a quiet day the busiest zone only hits ~75 raw-percentile.
    scale = _month_scale()
    quiet_day_best = max(score_on_scale(p, scale) for p in (20, 35, 50, 60))
    assert quiet_day_best < 83.0
    assert bucket_for_score(quiet_day_best) != "green"


def test_busy_day_top_zone_reaches_green():
    scale = _month_scale()
    busy_day_best = max(score_on_scale(p, scale) for p in (170, 185, 200))
    assert busy_day_best >= 83.0
    assert bucket_for_score(busy_day_best) == "green"


def test_order_within_a_frame_is_preserved_best_to_worst():
    # Monotonic: the busier zone always scores higher, so the map still ranks
    # best-to-worst — we only anchored the absolute LEVEL, not the ordering.
    scale = _month_scale()
    frame = [10, 45, 90, 130, 190]
    scored = [score_on_scale(p, scale) for p in frame]
    assert scored == sorted(scored)


def test_empty_scale_returns_none_so_caller_can_fall_back():
    assert score_on_scale(100, []) is None
    assert bucket_for_score(None) == "red"


def test_cutoffs_match_the_painted_map_thresholds():
    assert bucket_for_score(90) == "green"
    assert bucket_for_score(62) == "blue"
    assert bucket_for_score(45) == "yellow"
    assert bucket_for_score(10) == "red"


def test_tendency_and_colors_share_one_benchmark():
    # Tendency derives from the SAME absolute month scale the colors use, so the
    # two can't disagree. A zone running above its own monthly baseline reads
    # "above usual"; below it reads "below usual".
    scale = _month_scale()
    baseline = zone_month_average_score([40, 60, 50, 55], scale)   # a mid zone's usual
    now_hot = score_on_scale(180, scale)   # unusually busy for this zone right now
    now_cold = score_on_scale(20, scale)   # unusually quiet
    t_hot = tendency_from_benchmark(now_hot, baseline)
    t_cold = tendency_from_benchmark(now_cold, baseline)
    assert t_hot > 50 >= t_cold
    # Saturation deducts from tendency (busy-but-oversupplied reads lower).
    assert tendency_from_benchmark(now_hot, baseline, saturation_penalty=1.0) < t_hot


def test_tendency_none_when_no_baseline():
    assert tendency_from_benchmark(80.0, None) is None
    assert zone_month_average_score([], _month_scale()) is None
