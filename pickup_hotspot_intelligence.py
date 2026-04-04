from __future__ import annotations

import math
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from pyproj import Transformer
from shapely.geometry import Point, shape
from shapely.ops import transform, unary_union

_TO_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_TO_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


def _clip(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _timeslot_bin(ts: int, bin_minutes: int = 20) -> int:
    dt = time.gmtime(int(ts))
    return int((dt.tm_hour * 60 + dt.tm_min) // max(1, int(bin_minutes)))


def _timeslot_context_weight(frame_weekday: int, frame_bin: int, sample_ts: int, bin_minutes: int = 20) -> float:
    dt = time.gmtime(int(sample_ts))
    sample_weekday = int(dt.tm_wday)
    sample_bin = _timeslot_bin(sample_ts, bin_minutes=bin_minutes)
    delta = abs(sample_bin - frame_bin)

    if sample_weekday == frame_weekday and delta == 0:
        return 1.00
    if sample_weekday == frame_weekday and delta == 1:
        return 0.80
    if sample_weekday == frame_weekday and delta == 2:
        return 0.65
    if delta == 0:
        return 0.55
    return 0.35


def build_zone_historical_anchor_points(
    *,
    pickup_rows: Sequence[Mapping[str, Any]],
    frame_time: int,
    bin_minutes: int = 20,
) -> List[Dict[str, Any]]:
    frame_dt = time.gmtime(int(frame_time))
    frame_weekday = int(frame_dt.tm_wday)
    frame_bin = _timeslot_bin(frame_time, bin_minutes=bin_minutes)

    weighted: List[Dict[str, Any]] = []
    for row in pickup_rows:
        try:
            lng = float(row["lng"])
            lat = float(row["lat"])
            created_at = int(row.get("created_at") or frame_time)
            x, y = _TO_3857.transform(lng, lat)
        except Exception:
            continue
        weight = _timeslot_context_weight(frame_weekday, frame_bin, created_at, bin_minutes=bin_minutes)
        weighted.append(
            {
                "x": x,
                "y": y,
                "lng": lng,
                "lat": lat,
                "created_at": created_at,
                "weight": float(weight),
            }
        )
    return weighted


def build_zone_historical_anchor_components(
    *,
    zone_id: int,
    zone_geom: Any,
    weighted_points: Sequence[Mapping[str, Any]],
    cell_size_m: float = 135.0,
    sigma_m: float = 165.0,
    radius_m: float = 280.0,
    simplify_m: float = 18.0,
) -> List[Dict[str, Any]]:
    if zone_geom is None or getattr(zone_geom, "is_empty", True) or not weighted_points:
        return []
    zone_proj = transform(_TO_3857.transform, zone_geom)
    if zone_proj.is_empty:
        return []

    minx, miny, maxx, maxy = zone_proj.bounds
    start_x = minx + (cell_size_m / 2.0)
    start_y = miny + (cell_size_m / 2.0)
    cols = max(1, int(math.ceil((maxx - minx) / cell_size_m)))
    rows_n = max(1, int(math.ceil((maxy - miny) / cell_size_m)))
    radius_sq = radius_m * radius_m

    cell_scores: Dict[tuple[int, int], float] = {}
    peak_score = 0.0
    for gy in range(rows_n):
        cy = start_y + gy * cell_size_m
        for gx in range(cols):
            cx = start_x + gx * cell_size_m
            if not zone_proj.covers(Point(cx, cy)):
                continue
            score = 0.0
            for p in weighted_points:
                dx = cx - float(p["x"])
                dy = cy - float(p["y"])
                dist_sq = (dx * dx) + (dy * dy)
                if dist_sq > radius_sq:
                    continue
                score += float(p["weight"]) * math.exp(-(dist_sq) / (2.0 * sigma_m * sigma_m))
            if score > 0.0:
                cell_scores[(gx, gy)] = score
                peak_score = max(peak_score, score)

    if peak_score <= 0.0:
        return []

    threshold = peak_score * 0.44
    selected = {k for k, v in cell_scores.items() if v >= threshold}
    if not selected:
        selected = {max(cell_scores.items(), key=lambda kv: kv[1])[0]}

    visited: set[tuple[int, int]] = set()
    raw_components: List[set[tuple[int, int]]] = []
    for seed in selected:
        if seed in visited:
            continue
        q = [seed]
        comp: set[tuple[int, int]] = set()
        while q:
            cur = q.pop()
            if cur in visited or cur not in selected:
                continue
            visited.add(cur)
            comp.add(cur)
            cx, cy = cur
            for nx in (cx - 1, cx, cx + 1):
                for ny in (cy - 1, cy, cy + 1):
                    if (nx, ny) in selected and (nx, ny) not in visited:
                        q.append((nx, ny))
        if comp:
            raw_components.append(comp)

    out: List[Dict[str, Any]] = []
    half = cell_size_m / 2.0
    for rank, comp in enumerate(raw_components):
        polys = []
        comp_score = sum(cell_scores.get(k, 0.0) for k in comp)
        comp_peak = max((cell_scores.get(k, 0.0) for k in comp), default=0.0)
        support_weight = 0.0
        support_count = 0
        for gx, gy in comp:
            cx = start_x + gx * cell_size_m
            cy = start_y + gy * cell_size_m
            polys.append(
                Point(cx, cy).buffer(max(half * 0.92, 30.0), cap_style=3)
            )
        geom = unary_union(polys).intersection(zone_proj)
        if geom.is_empty:
            continue
        for p in weighted_points:
            if geom.buffer(max(35.0, half * 0.65)).covers(Point(float(p["x"]), float(p["y"]))):
                support_weight += float(p["weight"])
                support_count += 1
        simplified = geom.simplify(simplify_m, preserve_topology=True).intersection(zone_proj)
        centroid = simplified.centroid if not simplified.is_empty else geom.centroid
        out.append(
            {
                "anchor_rank": rank,
                "anchor_id": f"zone:{zone_id}:anchor:{rank}",
                "cells": comp,
                "polygon_proj": simplified if not simplified.is_empty else geom,
                "component_score": float(comp_score),
                "peak_score": float(comp_peak),
                "weighted_point_count": float(support_weight),
                "point_count": int(support_count),
                "centroid_x": float(centroid.x),
                "centroid_y": float(centroid.y),
            }
        )

    out.sort(
        key=lambda c: (
            float(c.get("component_score") or 0.0),
            float(c.get("weighted_point_count") or 0.0),
            int(c.get("point_count") or 0),
        ),
        reverse=True,
    )
    for i, comp in enumerate(out):
        comp["anchor_rank"] = i
        comp["anchor_id"] = f"zone:{zone_id}:anchor:{i}"
    return out


def determine_zone_hotspot_limit(zone_geom: Any, historical_components: Sequence[Mapping[str, Any]]) -> int:
    if zone_geom is None or getattr(zone_geom, "is_empty", True):
        return 2
    if len(historical_components) < 3:
        return 2

    zone_proj = transform(_TO_3857.transform, zone_geom)
    area_sq_mi = float(zone_proj.area) / 2_589_988.11
    total_weighted_support = sum(float(c.get("weighted_point_count") or 0.0) for c in historical_components)
    strongest = max(0.001, float(historical_components[0].get("component_score") or 0.001))
    third = historical_components[2]
    third_ratio = float(third.get("component_score") or 0.0) / strongest
    third_weighted = float(third.get("weighted_point_count") or 0.0)

    c0 = historical_components[0].get("polygon_proj")
    c1 = historical_components[1].get("polygon_proj")
    c2 = third.get("polygon_proj")
    if c0 is None or c1 is None or c2 is None:
        return 2
    p0 = c0.centroid
    p1 = c1.centroid
    p2 = c2.centroid
    sep_miles = min(p2.distance(p0), p2.distance(p1)) / 1609.344

    qualifies = (
        area_sq_mi >= 2.25
        and total_weighted_support >= 36.0
        and third_ratio >= 0.42
        and third_weighted >= 10.0
        and sep_miles >= 0.35
    )
    return 3 if qualifies else 2


def sculpt_hotspot_shapes_from_recent_points(
    historical_components: Sequence[Mapping[str, Any]],
    recent_points: Sequence[Mapping[str, Any]],
    zone_geom: Any,
    frame_time: int,
    drift_cap_m: float = 180.0,
) -> List[Dict[str, Any]]:
    if not historical_components or zone_geom is None or getattr(zone_geom, "is_empty", True):
        return []
    zone_proj = transform(_TO_3857.transform, zone_geom)
    if zone_proj.is_empty:
        return []

    recent_proj: List[Dict[str, float]] = []
    for row in recent_points:
        try:
            lng = float(row["lng"])
            lat = float(row["lat"])
            created_at = int(row.get("created_at") or frame_time)
            age_m = max(0.0, (frame_time - created_at) / 60.0)
            weight = max(0.05, 1.0 - (age_m / 90.0))
            x, y = _TO_3857.transform(lng, lat)
            recent_proj.append({"x": x, "y": y, "w": weight})
        except Exception:
            continue

    sculpted: List[Dict[str, Any]] = []
    for comp in historical_components:
        poly = comp.get("polygon_proj")
        if poly is None or poly.is_empty:
            continue
        centroid = poly.centroid
        close = []
        for pt in recent_proj:
            d = Point(pt["x"], pt["y"]).distance(centroid)
            if d <= 380.0:
                close.append((pt, d))

        shift_x = float(centroid.x)
        shift_y = float(centroid.y)
        recent_strength = 0.0
        if close:
            total_w = 0.0
            sx = 0.0
            sy = 0.0
            for pt, dist in close:
                radial = max(0.1, 1.0 - (dist / 380.0))
                w = pt["w"] * radial
                total_w += w
                sx += pt["x"] * w
                sy += pt["y"] * w
            if total_w > 0:
                target_x = sx / total_w
                target_y = sy / total_w
                dx = target_x - centroid.x
                dy = target_y - centroid.y
                drift = math.sqrt((dx * dx) + (dy * dy))
                if drift > drift_cap_m and drift > 0:
                    scale = drift_cap_m / drift
                    dx *= scale
                    dy *= scale
                shift_x = float(centroid.x + dx)
                shift_y = float(centroid.y + dy)
                recent_strength = min(1.0, total_w / 8.0)

        shifted = transform(lambda x, y, z=None: (x + (shift_x - centroid.x), y + (shift_y - centroid.y)), poly)
        expand = 20.0 + (48.0 * recent_strength)
        contract = 16.0 + (18.0 * (1.0 - recent_strength))
        shaped = shifted.buffer(expand).buffer(-contract).intersection(zone_proj)
        if shaped.is_empty:
            shaped = shifted.intersection(zone_proj)
        if shaped.is_empty:
            continue

        ll = transform(_TO_4326.transform, shaped)
        intensity = _clip(0.28 + 0.58 * (_clip(float(comp.get("component_score") or 0.0) / 10.0) * 0.75 + recent_strength * 0.25))
        sculpted.append(
            {
                **comp,
                "polygon_proj": shaped,
                "geometry": ll,
                "recent_shape_component": float(recent_strength),
                "intensity": float(intensity),
            }
        )
    return sculpted


def convert_historical_components_to_emittable_shapes(
    historical_components: Sequence[Mapping[str, Any]],
    zone_geom: Any,
) -> List[Dict[str, Any]]:
    if not historical_components or zone_geom is None or getattr(zone_geom, "is_empty", True):
        return []
    zone_proj = transform(_TO_3857.transform, zone_geom)
    if zone_proj.is_empty:
        return []

    converted: List[Dict[str, Any]] = []
    for comp in historical_components:
        poly = comp.get("polygon_proj")
        if poly is None or poly.is_empty:
            continue
        clipped = poly.intersection(zone_proj)
        if clipped.is_empty:
            continue
        shaped = clipped.buffer(18.0).buffer(-8.0).intersection(zone_proj)
        if shaped.is_empty:
            shaped = clipped
        if shaped.is_empty:
            continue
        simplified = shaped.simplify(14.0, preserve_topology=True).intersection(zone_proj)
        if not simplified.is_empty:
            shaped = simplified
        ll = transform(_TO_4326.transform, shaped)
        support = float(comp.get("weighted_point_count") or 0.0)
        comp_score = float(comp.get("component_score") or 0.0)
        support_norm = _clip(support / 14.0)
        score_norm = _clip(comp_score / 9.0)
        normalized = (score_norm * 0.62) + (support_norm * 0.38)
        intensity = _clip(0.24 + (0.52 * normalized), 0.20, 0.90)
        confidence = _clip(0.34 + (0.56 * normalized), 0.28, 0.95)
        converted.append(
            {
                **comp,
                "polygon_proj": shaped,
                "geometry": ll,
                "recent_shape_component": 0.0,
                "intensity": float(intensity),
                "confidence": float(confidence),
            }
        )
    return converted


def _feature_or_component_metric(comp: Mapping[str, Any], *keys: str, default: float = 0.0) -> float:
    props = comp.get("properties") if isinstance(comp, Mapping) else None
    for key in keys:
        value = None
        if isinstance(comp, Mapping) and key in comp:
            value = comp.get(key)
        elif isinstance(props, Mapping) and key in props:
            value = props.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return float(default)


def _component_geometry_proj(comp: Mapping[str, Any]):
    poly = comp.get("polygon_proj")
    if poly is not None and not poly.is_empty:
        return poly
    geom_obj = comp.get("geometry")
    if geom_obj is None and isinstance(comp.get("properties"), Mapping):
        geom_obj = comp.get("properties", {}).get("geometry")
    if geom_obj is None:
        return None
    try:
        if isinstance(geom_obj, dict):
            if geom_obj.get("type") == "Feature" and isinstance(geom_obj.get("geometry"), dict):
                geom_obj = geom_obj.get("geometry")
            geom_obj = shape(geom_obj)
        return transform(_TO_3857.transform, geom_obj)
    except Exception:
        return None


def should_merge_adjacent_zone_hotspots(
    zone_a_meta: Mapping[str, Any],
    zone_b_meta: Mapping[str, Any],
    comp_a: Mapping[str, Any],
    comp_b: Mapping[str, Any],
    recent_points_a: Sequence[Mapping[str, Any]],
    recent_points_b: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    borough_a = str(zone_a_meta.get("borough") or "").strip().lower()
    borough_b = str(zone_b_meta.get("borough") or "").strip().lower()
    if not borough_a or borough_a != borough_b:
        return {"eligible": False, "reason": "borough_mismatch"}

    zone_geom_a = zone_a_meta.get("geometry")
    zone_geom_b = zone_b_meta.get("geometry")
    if zone_geom_a is None or zone_geom_b is None or zone_geom_a.is_empty or zone_geom_b.is_empty:
        return {"eligible": False, "reason": "zone_geometry_missing"}

    zone_a_proj = transform(_TO_3857.transform, zone_geom_a)
    zone_b_proj = transform(_TO_3857.transform, zone_geom_b)
    boundary_distance_m = float(zone_a_proj.distance(zone_b_proj))
    if boundary_distance_m > 60.0:
        return {"eligible": False, "reason": "zone_boundary_too_far", "boundary_distance_m": boundary_distance_m}

    poly_a = _component_geometry_proj(comp_a)
    poly_b = _component_geometry_proj(comp_b)
    if poly_a is None or poly_b is None or poly_a.is_empty or poly_b.is_empty:
        return {"eligible": False, "reason": "component_geometry_missing"}

    centroid_distance_miles = float(poly_a.centroid.distance(poly_b.centroid)) / 1609.344
    if centroid_distance_miles > 0.40:
        return {"eligible": False, "reason": "centroids_too_far", "centroid_distance_miles": centroid_distance_miles}

    boundary_band = zone_a_proj.buffer(75.0).intersection(zone_b_proj.buffer(75.0))
    boundary_recent_count = 0
    for row in list(recent_points_a) + list(recent_points_b):
        try:
            x, y = _TO_3857.transform(float(row["lng"]), float(row["lat"]))
        except Exception:
            continue
        if not boundary_band.is_empty and boundary_band.covers(Point(x, y)):
            boundary_recent_count += 1

    if boundary_recent_count < 3:
        return {"eligible": False, "reason": "weak_boundary_activity", "boundary_recent_count": boundary_recent_count}

    combined_recent_count = len(recent_points_a) + len(recent_points_b)
    if combined_recent_count < 5:
        return {"eligible": False, "reason": "insufficient_recent_points", "combined_recent_count": combined_recent_count}

    support_a = _feature_or_component_metric(
        comp_a,
        "weighted_support",
        "weighted_point_count",
        "historical_weighted_support",
        "component_point_count",
        "sample_size",
        default=0.0,
    )
    support_b = _feature_or_component_metric(
        comp_b,
        "weighted_support",
        "weighted_point_count",
        "historical_weighted_support",
        "component_point_count",
        "sample_size",
        default=0.0,
    )
    point_count_a = _feature_or_component_metric(
        comp_a,
        "point_count",
        "component_point_count",
        "weighted_point_count",
        "historical_weighted_support",
        "sample_size",
        default=0.0,
    )
    point_count_b = _feature_or_component_metric(
        comp_b,
        "point_count",
        "component_point_count",
        "weighted_point_count",
        "historical_weighted_support",
        "sample_size",
        default=0.0,
    )
    combined_support = support_a + support_b
    if combined_support < 14.0:
        return {"eligible": False, "reason": "insufficient_historical_support", "combined_support": combined_support}

    score_a = _feature_or_component_metric(comp_a, "component_score", "peak_score", "intensity", default=0.0)
    score_b = _feature_or_component_metric(comp_b, "component_score", "peak_score", "intensity", default=0.0)
    merged_score = score_a + score_b
    if merged_score < (max(score_a, score_b) * 1.05):
        return {"eligible": False, "reason": "merged_score_not_stronger", "merged_score": merged_score}

    return {
        "eligible": True,
        "reason": "eligible",
        "boundary_distance_m": boundary_distance_m,
        "centroid_distance_miles": centroid_distance_miles,
        "boundary_recent_count": boundary_recent_count,
        "combined_recent_count": combined_recent_count,
        "combined_support": combined_support,
        "combined_point_count": point_count_a + point_count_b,
        "merged_score": merged_score,
    }


def make_cross_zone_merged_geometry(
    comp_a: Mapping[str, Any],
    comp_b: Mapping[str, Any],
    zone_geom_a: Any,
    zone_geom_b: Any,
):
    poly_a = _component_geometry_proj(comp_a)
    poly_b = _component_geometry_proj(comp_b)
    if poly_a is None or poly_b is None or poly_a.is_empty or poly_b.is_empty:
        return None

    zone_union_proj = unary_union(
        [transform(_TO_3857.transform, zone_geom_a), transform(_TO_3857.transform, zone_geom_b)]
    )
    merged = unary_union([poly_a, poly_b]).intersection(zone_union_proj)
    if merged.is_empty:
        return None
    simplified = merged.simplify(16.0, preserve_topology=True).intersection(zone_union_proj)
    return simplified if not simplified.is_empty else merged


def build_cross_zone_merged_hotspots(
    *,
    zone_feature_map: Mapping[int, Sequence[Mapping[str, Any]]],
    zone_meta_map: Mapping[int, Mapping[str, Any]],
    zone_recent_points: Mapping[int, Sequence[Mapping[str, Any]]],
) -> List[Dict[str, Any]]:
    zone_ids = sorted(int(z) for z in zone_feature_map.keys())
    merged: List[Dict[str, Any]] = []
    for i, zone_a in enumerate(zone_ids):
        features_a = list(zone_feature_map.get(zone_a) or [])
        if not features_a:
            continue
        for zone_b in zone_ids[i + 1 :]:
            features_b = list(zone_feature_map.get(zone_b) or [])
            if not features_b:
                continue
            meta_a = zone_meta_map.get(zone_a) or {}
            meta_b = zone_meta_map.get(zone_b) or {}
            comp_a = features_a[0]
            comp_b = features_b[0]
            decision = should_merge_adjacent_zone_hotspots(
                meta_a,
                meta_b,
                comp_a,
                comp_b,
                zone_recent_points.get(zone_a) or [],
                zone_recent_points.get(zone_b) or [],
            )
            if not decision.get("eligible"):
                continue
            merged_geom_proj = make_cross_zone_merged_geometry(
                comp_a,
                comp_b,
                meta_a.get("geometry"),
                meta_b.get("geometry"),
            )
            if merged_geom_proj is None or merged_geom_proj.is_empty:
                continue
            a, b = sorted([zone_a, zone_b])
            merged.append(
                {
                    "zone_pair": (a, b),
                    "merged_geometry_proj": merged_geom_proj,
                    "geometry": transform(_TO_4326.transform, merged_geom_proj),
                    "primary_zone_id": a,
                    "merged_zone_ids": [a, b],
                    "merged_zone_names": [
                        str((zone_meta_map.get(a) or {}).get("zone_name") or ""),
                        str((zone_meta_map.get(b) or {}).get("zone_name") or ""),
                    ],
                    "borough": str(meta_a.get("borough") or meta_b.get("borough") or ""),
                    "component_a": comp_a,
                    "component_b": comp_b,
                    "decision": decision,
                }
            )
    merged.sort(key=lambda r: float((r.get("decision") or {}).get("merged_score") or 0.0), reverse=True)
    return merged


def get_zone_or_hotspot_outcome_modifier(
    outcome_rows: Iterable[Mapping[str, Any]],
    *,
    min_samples: int = 6,
) -> Dict[str, float]:
    rows = list(outcome_rows)
    sample_count = len(rows)
    if sample_count < max(1, min_samples):
        return {"modifier": 1.0, "sample_count": float(sample_count), "conversion_rate": 0.0, "median_minutes_to_trip": 0.0}

    converted = 0
    mins: List[float] = []
    for row in rows:
        flag = row.get("converted_to_trip")
        if flag in (1, True, "1", "true", "TRUE"):
            converted += 1
        mt = row.get("minutes_to_trip")
        if mt is not None:
            try:
                mins.append(float(mt))
            except Exception:
                pass
    conversion_rate = converted / max(1, sample_count)
    mins.sort()
    median_minutes = mins[len(mins) // 2] if mins else 15.0

    conv_boost = (conversion_rate - 0.45) * 0.55
    speed_boost = (12.0 - median_minutes) / 50.0
    modifier = _clip(1.0 + conv_boost + speed_boost, 0.78, 1.22)
    return {
        "modifier": float(modifier),
        "sample_count": float(sample_count),
        "conversion_rate": float(conversion_rate),
        "median_minutes_to_trip": float(median_minutes),
    }


def build_hotspot_quality_modifier(*, short_trip_share: float = 0.0, continuation_score: float = 0.0, saturation: float = 0.0, borough: str = "") -> Dict[str, float]:
    trap_penalty = _clip(short_trip_share, 0.0, 1.0) * 0.18
    continuation_bonus = _clip(continuation_score, 0.0, 1.0) * 0.10
    saturation_penalty = _clip(saturation, 0.0, 1.0) * (0.20 if str(borough).lower() == "manhattan" else 0.14)
    modifier = _clip(1.0 - trap_penalty - saturation_penalty + continuation_bonus, 0.72, 1.18)
    return {
        "quality_modifier": float(modifier),
        "short_trip_trap_penalty": float(trap_penalty),
        "continuation_bonus": float(continuation_bonus),
        "saturation_penalty": float(saturation_penalty),
    }
