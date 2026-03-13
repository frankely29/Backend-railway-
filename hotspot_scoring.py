from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional

from hotspot_models import ZoneScoreResult


def recency_decay_weight(age_seconds: float) -> float:
    age_minutes = max(0.0, age_seconds / 60.0)
    if age_minutes <= 10:
        return 1.0
    if age_minutes <= 20:
        return 0.78
    if age_minutes <= 40:
        return 0.5
    if age_minutes <= 60:
        return 0.3
    if age_minutes <= 120:
        return 0.12
    return 0.04


def _clip(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _normalized_strength(weighted_trip_count: float, unique_driver_count: int, same_timeslot_support: float, active_driver_count: int) -> float:
    trip_strength = _clip(weighted_trip_count / 14.0)
    unique_strength = _clip(unique_driver_count / 7.0)
    timeslot_strength = _clip(same_timeslot_support / 8.0)
    network_strength = _clip(active_driver_count / 80.0)
    return _clip(0.40 * trip_strength + 0.25 * unique_strength + 0.20 * timeslot_strength + 0.15 * network_strength)


def score_zones(
    *,
    now_ts: int,
    zone_points: Mapping[int, List[Dict[str, Any]]],
    historical_by_zone: Mapping[int, float],
    same_timeslot_by_zone: Mapping[int, float],
    density_by_zone: Mapping[int, float],
    active_driver_count: int,
    previous_scores: Optional[Mapping[int, float]] = None,
) -> Dict[int, ZoneScoreResult]:
    previous_scores = previous_scores or {}
    out: Dict[int, ZoneScoreResult] = {}

    all_zone_ids = set(zone_points.keys()) | set(historical_by_zone.keys()) | set(same_timeslot_by_zone.keys())
    adaptive_threshold = 1.6 + min(2.0, max(0.0, active_driver_count / 65.0))

    for zid in all_zone_ids:
        points = zone_points.get(zid, [])
        weighted_trip_count = 0.0
        unique_drivers: set[int] = set()

        for p in points:
            created_at = int(p.get("created_at") or now_ts)
            age = max(0, now_ts - created_at)
            weighted_trip_count += recency_decay_weight(age)
            uid = p.get("user_id")
            if uid is not None:
                try:
                    unique_drivers.add(int(uid))
                except Exception:
                    pass

        unique_driver_count = len(unique_drivers)
        diversity = 0.30 + 0.70 * _clip(unique_driver_count / max(1.0, weighted_trip_count)) if weighted_trip_count > 0 else 0.0
        adjusted_live = weighted_trip_count * diversity

        historical_norm = _clip((historical_by_zone.get(zid, 0.0) or 0.0) / 16.0)
        live_norm = _clip(adjusted_live / 14.0)
        same_timeslot_norm = _clip((same_timeslot_by_zone.get(zid, 0.0) or 0.0) / 10.0)
        density_penalty = _clip((density_by_zone.get(zid, 0.0) or 0.0) / 6.0)

        strength = _normalized_strength(adjusted_live, unique_driver_count, same_timeslot_by_zone.get(zid, 0.0) or 0.0, active_driver_count)

        hist_w = 0.58 - 0.18 * strength
        live_w = 0.20 + 0.10 * strength
        timeslot_w = 0.17 + 0.03 * strength
        density_w = 0.08

        raw = (hist_w * historical_norm) + (live_w * live_norm) + (timeslot_w * same_timeslot_norm) - (density_w * density_penalty)

        confidence = _clip(0.25 + 0.75 * strength)
        gated = max(0.0, raw) * confidence

        prev_score = float(previous_scores.get(zid, gated))
        smoothed = (0.65 * prev_score) + (0.35 * gated)

        min_evidence = adjusted_live >= adaptive_threshold or historical_norm >= 0.25
        recommended = bool(min_evidence and confidence >= 0.38 and smoothed >= 0.20)

        out[zid] = ZoneScoreResult(
            zone_id=int(zid),
            final_score=_clip(smoothed),
            confidence=confidence,
            live_strength=strength,
            density_penalty=density_penalty,
            historical_component=hist_w * historical_norm,
            live_component=live_w * live_norm,
            same_timeslot_component=timeslot_w * same_timeslot_norm,
            weighted_trip_count=adjusted_live,
            unique_driver_count=unique_driver_count,
            recommended=recommended,
        )
    return out
