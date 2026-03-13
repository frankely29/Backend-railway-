from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Tuple

from pyproj import Transformer

from hotspot_models import MicroHotspotScoreResult
from hotspot_scoring import recency_decay_weight

_TO_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)


def _clip(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def score_micro_hotspots(
    *,
    now_ts: int,
    zone_id: int,
    point_rows: List[Dict[str, Any]],
    historical_zone_support: float,
    same_timeslot_support: float,
    density_penalty: float,
    top_n: int = 3,
) -> List[MicroHotspotScoreResult]:
    if not point_rows:
        return []

    grid_m = 90.0
    clusters: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)

    for p in point_rows:
        try:
            lng = float(p["lng"])
            lat = float(p["lat"])
        except Exception:
            continue
        x, y = _TO_3857.transform(lng, lat)
        gx = int(math.floor(x / grid_m))
        gy = int(math.floor(y / grid_m))
        enriched = dict(p)
        enriched["_x"] = x
        enriched["_y"] = y
        clusters[(gx, gy)].append(enriched)

    results: List[MicroHotspotScoreResult] = []
    for (gx, gy), points in clusters.items():
        weighted = 0.0
        unique_drivers: set[int] = set()
        sx = sy = slat = slng = 0.0

        for p in points:
            created_at = int(p.get("created_at") or now_ts)
            w = recency_decay_weight(max(0, now_ts - created_at))
            weighted += w
            sx += p["_x"] * w
            sy += p["_y"] * w
            slat += float(p["lat"]) * w
            slng += float(p["lng"]) * w
            uid = p.get("user_id")
            if uid is not None:
                try:
                    unique_drivers.add(int(uid))
                except Exception:
                    pass

        if weighted <= 0:
            continue

        center_x = sx / weighted
        center_y = sy / weighted
        center_lat = slat / weighted
        center_lng = slng / weighted

        spread_sum = 0.0
        for p in points:
            spread_sum += math.sqrt((p["_x"] - center_x) ** 2 + (p["_y"] - center_y) ** 2)
        spread_m = spread_sum / max(1, len(points))
        radius_m = max(60.0, min(120.0, 65.0 + spread_m * 0.8))

        unique_count = len(unique_drivers)
        diversity = 0.30 + 0.70 * _clip(unique_count / max(1.0, weighted))
        weighted_adj = weighted * diversity

        baseline_component = 0.56 * _clip(historical_zone_support / 14.0)
        live_component = 0.24 * _clip(weighted_adj / 7.0)
        timeslot_component = 0.17 * _clip(same_timeslot_support / 8.0)
        crowding_component = 0.08 * _clip(density_penalty)

        # ETA-aware approximation: clusters with effective approach proxy near 5 minutes get a slight boost.
        effective_reach_m = radius_m + (spread_m * 1.5)
        eta_minutes = effective_reach_m / 250.0
        eta_alignment = math.exp(-((eta_minutes - 5.0) ** 2) / (2 * 2.2 * 2.2))

        raw = (baseline_component + live_component + timeslot_component - crowding_component) * (0.85 + 0.15 * eta_alignment)
        confidence = _clip(0.22 + 0.38 * _clip(weighted_adj / 6.0) + 0.25 * _clip(unique_count / 4.0) + 0.15 * _clip(same_timeslot_support / 6.0))
        final_score = _clip(max(0.0, raw) * confidence)

        results.append(
            MicroHotspotScoreResult(
                cluster_id=f"z{zone_id}_{gx}_{gy}",
                zone_id=zone_id,
                center_lat=center_lat,
                center_lng=center_lng,
                radius_m=radius_m,
                intensity=final_score,
                confidence=confidence,
                weighted_trip_count=weighted_adj,
                unique_driver_count=unique_count,
                crowding_penalty=_clip(density_penalty),
                baseline_component=baseline_component,
                live_component=live_component,
                same_timeslot_component=timeslot_component,
                final_score=final_score,
                # Low-volume experiment tuning: surface micro-hotspots earlier while confidence/diversity logic still guards noise.
                recommended=bool(weighted_adj >= 0.85 and unique_count >= 1 and confidence >= 0.18 and final_score >= 0.10),  # Low-volume bootstrap tuning for earlier live surfacing.
                eta_alignment=eta_alignment,
            )
        )

    results.sort(key=lambda r: r.final_score, reverse=True)
    cap = max(1, min(3, int(top_n)))
    recommended = [r for r in results if r.recommended]
    if recommended:
        return recommended[:cap]

    # Conservative low-volume fallback: only surface near-threshold clusters when
    # strict recommendation would otherwise produce empty experimental output.
    fallback = [
        r
        for r in results
        if r.unique_driver_count >= 1 and r.confidence >= 0.10 and r.final_score >= 0.06  # Low-volume bootstrap fallback.
    ]
    return fallback[:cap]
