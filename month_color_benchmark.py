"""Month-anchored color scale — make a color mean the same busy-ness every day.

FLAW this fixes: the visible rating is a per-frame percentile rank (zones ranked
against ONLY the other zones in the same 20-min window). So the best zone in every
frame is ~100 -> green, no matter how busy the day actually is. Today's green and
yesterday's green are therefore different absolute busy-ness — the color is
inconsistent day to day (and the same is true down the whole scale).

FIX (this module): build ONE benchmark per month from the RAW demand signal
(pickups), then score every zone-frame against that month-wide distribution
instead of re-normalizing inside each frame. Now the score is ABSOLUTE and
consistent: green is only reached when a zone is genuinely as busy as a
green-level day for the month. On a quiet day (e.g. July 5) the best zone might
top out at 75 and NOT qualify for green — exactly the desired behavior. Rank
order within a frame is preserved (the mapping is monotonic in demand), so the
map still shows best-to-worst; only the absolute level is anchored to the month.

The Tendency benchmark can't do this: it averages the already-ranked rating, so
the absolute demand is gone before it runs. This benchmark is built from the raw
pickups the shadow rows persist, so it keeps the absolute information.

Pure/stdlib-only so it is fully unit-testable without the data volume.
"""
from __future__ import annotations

import bisect
import math
from typing import Iterable, List, Optional, Sequence

# The color cutoffs are the SAME absolute thresholds already used to paint the
# map (green >= 83 ... red < 30). Anchoring the score to the month means these
# now carry a consistent meaning across days instead of being re-hit every frame.
COLOR_CUTOFFS = [
    (83.0, "green"), (75.0, "purple"), (68.0, "indigo"), (60.0, "blue"),
    (50.0, "sky"), (40.0, "yellow"), (30.0, "orange"), (0.0, "red"),
]


def _demand_signal(raw_pickups: float) -> float:
    """The busy-ness signal we rank on. LN(1+pickups) matches the scoring engine's
    demand transform (diminishing returns on raw counts), so the month scale lines
    up with how the engine already weighs demand."""
    p = max(0.0, float(raw_pickups or 0.0))
    return math.log1p(p)


def build_month_demand_scale(raw_pickups_values: Iterable[float]) -> List[float]:
    """Sorted month-wide distribution of the demand signal — the benchmark.

    Built once when the month's data is built, from every zone-frame's raw
    pickups. `score_on_scale` places any zone against it. Empty when there's no
    data (caller falls back to the per-frame rating)."""
    scale = sorted(
        _demand_signal(v) for v in raw_pickups_values if v is not None
    )
    return scale


def score_on_scale(raw_pickups: float, sorted_scale: Sequence[float]) -> Optional[float]:
    """Absolute 0-100 score: the month-wide percentile of this zone's demand.

    Same raw pickups -> same score on any day (the scale is the whole month), so
    a color means the same busy-ness every day. Monotonic in demand, so within a
    frame the busier zone still scores higher (best-to-worst preserved)."""
    n = len(sorted_scale)
    if n == 0:
        return None
    v = _demand_signal(raw_pickups)
    lo = bisect.bisect_left(sorted_scale, v)
    hi = bisect.bisect_right(sorted_scale, v)
    # Mid-rank percentile (average of lower/upper bounds for ties), 0..100.
    rank = (lo + hi) / 2.0
    return max(0.0, min(100.0, 100.0 * rank / n))


def bucket_for_score(score: Optional[float]) -> str:
    if score is None:
        return "red"
    for cutoff, name in COLOR_CUTOFFS:
        if score >= cutoff:
            return name
    return "red"


# --- Shared with Tendency ----------------------------------------------------
# Tendency and the map colors must read from ONE monthly benchmark. The colors
# use `score_on_scale` (absolute placement on the month). Tendency needs a zone's
# OWN monthly baseline; we compute it on the SAME absolute scale below, so the two
# features can never drift apart. (Today's Tendency instead averages the per-frame
# RANK — a different footing from the colors; this unifies them on raw demand.)

TENDENCY_SATURATION_POINTS = 16.0


def zone_month_average_score(
    zone_raw_pickups: Iterable[float], sorted_scale: Sequence[float]
) -> Optional[float]:
    """A zone's own monthly-average ABSOLUTE score — its baseline for Tendency.

    Averages the zone's per-frame absolute scores over the month, on the same
    month scale the colors use. None when the zone has no data."""
    scores = [
        s for s in (score_on_scale(p, sorted_scale) for p in zone_raw_pickups if p is not None)
        if s is not None
    ]
    if not scores:
        return None
    return sum(scores) / len(scores)


def tendency_from_benchmark(
    current_absolute_score: Optional[float],
    zone_month_average_score: Optional[float],
    saturation_penalty: float = 0.0,
) -> Optional[float]:
    """Tendency on the SAME benchmark the colors use: how busy the zone is right
    now vs its OWN monthly usual, centered at 50, minus a saturation deduction.

    Because `current_absolute_score` is the month-anchored color score and the
    baseline is that same score averaged over the month, Tendency and the map are
    guaranteed consistent — a zone can't read "above usual" while painting a color
    that says otherwise."""
    if current_absolute_score is None or zone_month_average_score is None:
        return None
    sat = max(0.0, min(1.0, float(saturation_penalty or 0.0))) * TENDENCY_SATURATION_POINTS
    return max(0.0, min(100.0, 50.0 + (current_absolute_score - zone_month_average_score) - sat))
