from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from hotspot_models import ZoneScoreResult


def recency_decay_weight(age_seconds: float) -> float:
    age_minutes = max(0.0, age_seconds / 60.0)
    if age_minutes <= 12:
        return 1.0
    if age_minutes <= 30:
        return 0.65
    if age_minutes <= 60:
        return 0.34
    if age_minutes <= 120:
        return 0.15
    return 0.06


def _clip(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


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
        diversity = 0.32 + 0.68 * _clip(unique_driver_count / max(1.0, weighted_trip_count)) if weighted_trip_count > 0 else 0.0
        adjusted_live = weighted_trip_count * diversity

        long_run_hist = float(historical_by_zone.get(zid, 0.0) or 0.0)
        continuation = float(same_timeslot_by_zone.get(zid, 0.0) or 0.0)
        saturation = _clip(float(density_by_zone.get(zid, 0.0) or 0.0) / 5.0)
        active_network = _clip(active_driver_count / 90.0)

        hist_norm = _clip(long_run_hist / 52.0)
        live_norm = _clip(adjusted_live / 12.0)
        continuation_norm = _clip(continuation / 28.0)
        network_norm = _clip((active_network * 0.55) + (live_norm * 0.45))

        long_run_component = 0.55 * hist_norm
        live_component = 0.20 * live_norm
        continuation_component = 0.15 * continuation_norm
        network_component = 0.10 * network_norm
        saturation_penalty = 0.18 * saturation

        raw = long_run_component + live_component + continuation_component + network_component - saturation_penalty
        confidence = _clip(0.30 + (0.60 * hist_norm) + (0.20 * live_norm) - (0.10 * saturation))
        gated = max(0.0, raw) * confidence

        prev_score = float(previous_scores.get(zid, gated))
        smoothed = (0.72 * prev_score) + (0.28 * gated)

        min_evidence = (hist_norm >= 0.24) or (live_norm >= 0.15 and continuation_norm >= 0.12)
        recommended = bool(min_evidence and confidence >= 0.36 and smoothed >= 0.18)

        out[zid] = ZoneScoreResult(
            zone_id=int(zid),
            final_score=_clip(smoothed),
            confidence=confidence,
            live_strength=live_norm,
            density_penalty=saturation,
            historical_component=long_run_component,
            live_component=live_component,
            same_timeslot_component=continuation_component,
            long_run_historical_component=long_run_component,
            recent_shape_component=live_component,
            outcome_modifier=1.0,
            quality_modifier=1.0,
            saturation_modifier=1.0 - saturation_penalty,
            hotspot_limit_used=0,
            weighted_trip_count=adjusted_live,
            unique_driver_count=unique_driver_count,
            recommended=recommended,
            merged=False,
            merged_zone_count=1,
            hotspot_method="historical_anchor_sculpted",
            merged_zone_ids=None,
            covered_zone_ids=[int(zid)],
        )
    return out
