from __future__ import annotations

import json
import math
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from shapely.geometry import Point, shape

MOVE_NEARBY_COOLDOWN_SECONDS = 11 * 60
MICRO_REPOSITION_COOLDOWN_SECONDS = 7 * 60
MOVE_NEARBY_MIN_IMPROVEMENT = 10.0
RECENT_WINDOW_SECONDS = 2 * 3600

_zone_geometry_cache_lock = threading.Lock()
_zone_geometry_cache_mtime: Optional[float] = None
_zone_geometry_cache_path: Optional[str] = None
_zone_geometry_cache: Dict[int, Dict[str, Any]] = {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_haversine_miles(lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> float:
    try:
        la1 = float(lat1)
        ln1 = float(lng1)
        la2 = float(lat2)
        ln2 = float(lng2)
    except Exception:
        return 0.0
    radius_m = 6371000.0
    phi1 = math.radians(la1)
    phi2 = math.radians(la2)
    dphi = math.radians(la2 - la1)
    dlambda = math.radians(ln2 - ln1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_m * c * 0.000621371


def _load_zone_geometries(zones_geojson_path: Path) -> Dict[int, Dict[str, Any]]:
    global _zone_geometry_cache_mtime, _zone_geometry_cache_path, _zone_geometry_cache
    path = Path(zones_geojson_path)
    try:
        mtime = path.stat().st_mtime
    except Exception:
        return {}

    with _zone_geometry_cache_lock:
        if _zone_geometry_cache and _zone_geometry_cache_mtime == mtime and _zone_geometry_cache_path == str(path):
            return _zone_geometry_cache

        parsed: Dict[int, Dict[str, Any]] = {}
        raw = json.loads(path.read_text(encoding="utf-8"))
        for feature in raw.get("features", []):
            props = (feature or {}).get("properties") or {}
            geom_data = (feature or {}).get("geometry")
            if not geom_data:
                continue
            try:
                zone_id = int(props.get("LocationID"))
            except Exception:
                continue
            geom = shape(geom_data)
            if geom.is_empty:
                continue
            centroid = geom.centroid
            parsed[zone_id] = {
                "zone_id": zone_id,
                "zone_name": str(props.get("zone") or "").strip() or None,
                "borough": str(props.get("borough") or "").strip() or None,
                "geometry": geom,
                "centroid_lat": float(centroid.y),
                "centroid_lng": float(centroid.x),
            }

        _zone_geometry_cache = parsed
        _zone_geometry_cache_mtime = mtime
        _zone_geometry_cache_path = str(path)
        return _zone_geometry_cache


def resolve_current_zone_from_position(
    *,
    zones_geojson_path: Path,
    lat: float,
    lng: float,
) -> Dict[str, Any]:
    zones = _load_zone_geometries(Path(zones_geojson_path))
    if not zones:
        return {
            "current_zone_id": None,
            "current_zone_name": None,
            "current_borough": None,
            "nearest_zone_id": None,
            "nearest_zone_name": None,
            "nearest_zone_distance_miles": None,
        }

    point = Point(float(lng), float(lat))
    matched: Optional[Dict[str, Any]] = None
    nearest: Optional[Dict[str, Any]] = None
    nearest_dist = float("inf")

    for zone in zones.values():
        geom = zone.get("geometry")
        if geom is None:
            continue
        if matched is None and geom.covers(point):
            matched = zone
        dist = _safe_haversine_miles(lat, lng, zone.get("centroid_lat"), zone.get("centroid_lng"))
        if dist < nearest_dist:
            nearest_dist = dist
            nearest = zone

    zone_ref = matched or nearest
    return {
        "current_zone_id": int(zone_ref.get("zone_id")) if zone_ref else None,
        "current_zone_name": (zone_ref or {}).get("zone_name"),
        "current_borough": (zone_ref or {}).get("borough"),
        "nearest_zone_id": int(nearest.get("zone_id")) if nearest else None,
        "nearest_zone_name": (nearest or {}).get("zone_name"),
        "nearest_zone_distance_miles": None if nearest is None else round(float(nearest_dist), 3),
    }


def load_zone_centroid_lookup(zones_geojson_path: Path) -> Dict[int, Dict[str, Any]]:
    zones = _load_zone_geometries(Path(zones_geojson_path))
    return {
        int(zone_id): {
            "centroid_lat": data.get("centroid_lat"),
            "centroid_lng": data.get("centroid_lng"),
            "zone_name": data.get("zone_name"),
            "borough": data.get("borough"),
        }
        for zone_id, data in zones.items()
    }


def load_driver_activity_snapshot(
    *,
    user_id: int,
    now_ts: int,
    current_lat: Optional[float],
    current_lng: Optional[float],
    db_query_one,
    db_query_all,
) -> Dict[str, Any]:
    presence_row = db_query_one(
        "SELECT lat, lng, updated_at FROM presence WHERE user_id=? LIMIT 1",
        (int(user_id),),
    )
    guard_row = db_query_one(
        "SELECT movement_streak_started_at, last_meaningful_motion_at FROM pickup_guard_state WHERE user_id=? LIMIT 1",
        (int(user_id),),
    )
    latest_trip = db_query_one(
        """
        SELECT id, lat, lng, created_at
        FROM pickup_logs pl
        WHERE pl.user_id=?
          AND COALESCE(pl.is_voided, 0) IN (0, FALSE)
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id),),
    )
    counts_row = db_query_one(
        """
        SELECT
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c30,
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c60,
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c120
        FROM pickup_logs pl
        WHERE pl.user_id=?
          AND COALESCE(pl.is_voided, 0) IN (0, FALSE)
          AND created_at >= ?
        """,
        (int(now_ts) - 1800, int(now_ts) - 3600, int(now_ts) - 7200, int(user_id), int(now_ts) - 7200),
    )
    rec_rows = db_query_all(
        """
        SELECT converted_to_trip
        FROM recommendation_outcomes
        WHERE user_id=?
          AND converted_to_trip IS NOT NULL
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 25
        """,
        (int(user_id), int(now_ts) - RECENT_WINDOW_SECONDS),
    )
    micro_rows = db_query_all(
        """
        SELECT converted_to_trip
        FROM micro_recommendation_outcomes
        WHERE user_id=?
          AND converted_to_trip IS NOT NULL
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 25
        """,
        (int(user_id), int(now_ts) - RECENT_WINDOW_SECONDS),
    )
    state_row = db_query_one("SELECT * FROM driver_guidance_state WHERE user_id=? LIMIT 1", (int(user_id),))

    tripless_minutes = max(0.0, (float(now_ts) - _safe_float((latest_trip or {}).get("created_at"), float(now_ts))) / 60.0)
    movement_started_at = (guard_row or {}).get("movement_streak_started_at")
    last_motion_at = (guard_row or {}).get("last_meaningful_motion_at")

    stationary_minutes = 0.0
    movement_minutes = 0.0
    if last_motion_at is not None:
        stationary_minutes = max(0.0, (float(now_ts) - float(last_motion_at)) / 60.0)
    if movement_started_at is not None:
        movement_minutes = max(0.0, (float(now_ts) - float(movement_started_at)) / 60.0)

    moved_since_last_saved_trip = False
    if latest_trip is not None and current_lat is not None and current_lng is not None:
        moved_since_last_saved_trip = (
            _safe_haversine_miles(current_lat, current_lng, latest_trip.get("lat"), latest_trip.get("lng")) >= 0.25
        )

    rec_total = len(rec_rows)
    rec_conv = sum(1 for row in rec_rows if bool(row.get("converted_to_trip")))
    micro_total = len(micro_rows)
    micro_conv = sum(1 for row in micro_rows if bool(row.get("converted_to_trip")))
    rec_rate = (float(rec_conv) / float(rec_total)) if rec_total > 0 else 0.0
    micro_rate = (float(micro_conv) / float(micro_total)) if micro_total > 0 else 0.0

    recent_saved_trip_count_30m = _safe_int((counts_row or {}).get("c30"), 0)
    recent_saved_trip_count_60m = _safe_int((counts_row or {}).get("c60"), 0)
    recent_saved_trip_count_120m = _safe_int((counts_row or {}).get("c120"), 0)

    uncertainty = 0.2
    if tripless_minutes >= 25:
        uncertainty += 0.2
    if tripless_minutes >= 45:
        uncertainty += 0.2
    if rec_total >= 4 and rec_rate < 0.25:
        uncertainty += 0.15
    if micro_total >= 4 and micro_rate < 0.25:
        uncertainty += 0.15
    if recent_saved_trip_count_120m == 0:
        uncertainty += 0.1

    presence_updated = _safe_int((presence_row or {}).get("updated_at"), 0)
    current_presence_stale = (now_ts - presence_updated) > 300 if presence_updated > 0 else True

    return {
        "tripless_minutes": round(tripless_minutes, 2),
        "stationary_minutes": round(stationary_minutes, 2),
        "movement_minutes": round(movement_minutes, 2),
        "moved_since_last_saved_trip": bool(moved_since_last_saved_trip),
        "recent_saved_trip_count_30m": int(recent_saved_trip_count_30m),
        "recent_saved_trip_count_60m": int(recent_saved_trip_count_60m),
        "recent_saved_trip_count_120m": int(recent_saved_trip_count_120m),
        "recent_move_attempts_without_trip": _safe_int((state_row or {}).get("recent_move_attempts_without_trip"), 0),
        "recent_recommendation_conversion_rate": round(rec_rate, 3),
        "recent_micro_conversion_rate": round(micro_rate, 3),
        "dispatch_uncertainty": min(1.0, round(uncertainty, 3)),
        "current_presence_stale": bool(current_presence_stale),
        "guidance_state": state_row or {},
    }


def build_driver_guidance(
    *,
    user_id: int,
    frame_time: str,
    current_lat: float,
    current_lng: float,
    current_zone_id: int | None,
    current_zone_name: str | None,
    current_borough: str | None,
    mode_flags: dict[str, bool],
    assistant_outlook_bucket: dict[str, Any],
    activity_snapshot: dict[str, Any],
    zone_context: dict[str, Any],
    now_ts: int,
) -> dict[str, Any]:
    _ = user_id, frame_time, current_lat, current_lng, mode_flags, assistant_outlook_bucket
    tripless_minutes = _safe_float(activity_snapshot.get("tripless_minutes"))
    stationary_minutes = _safe_float(activity_snapshot.get("stationary_minutes"))
    movement_minutes = _safe_float(activity_snapshot.get("movement_minutes"))
    dispatch_uncertainty = _safe_float(activity_snapshot.get("dispatch_uncertainty"), 0.3)
    recent_move_attempts = _safe_int(activity_snapshot.get("recent_move_attempts_without_trip"), 0)
    recent_saved_60 = _safe_int(activity_snapshot.get("recent_saved_trip_count_60m"), 0)
    state = activity_snapshot.get("guidance_state") or {}

    current_zone = zone_context.get("current_zone") or {}
    nearby_candidates = zone_context.get("nearby_candidates") or []
    best_nearby = nearby_candidates[0] if nearby_candidates else None

    current_rating = _safe_float(current_zone.get("rating"), 0.0)
    current_next_rating = _safe_float(current_zone.get("next_rating"), current_rating)
    current_saturation_penalty = _safe_float(current_zone.get("market_saturation_penalty"), 0.0)
    current_continuation_raw = _safe_float(current_zone.get("continuation_raw"), 0.0)
    settling_window = tripless_minutes <= 18.0 or movement_minutes <= 12.0

    reason_codes: List[str] = []
    action = "hold"
    confidence = 0.55
    message = "Hold your line in this zone; setup still looks workable."
    target_zone: Optional[Dict[str, Any]] = None

    last_move_guidance_at = _safe_int(state.get("last_move_guidance_at"), 0)
    last_guidance_action = str(state.get("last_guidance_action") or "").strip().lower()
    move_cooldown_until_unix: Optional[int] = None
    if last_guidance_action in {"move_nearby", "micro_reposition"}:
        cooldown = MOVE_NEARBY_COOLDOWN_SECONDS if last_guidance_action == "move_nearby" else MICRO_REPOSITION_COOLDOWN_SECONDS
        move_cooldown_until_unix = last_move_guidance_at + cooldown if last_move_guidance_at > 0 else None

    in_move_cooldown = move_cooldown_until_unix is not None and now_ts < int(move_cooldown_until_unix)
    hold_until_unix: Optional[int] = None

    if current_rating >= 64 and current_next_rating >= (current_rating - 4) and current_continuation_raw >= 0.45 and settling_window:
        action = "hold"
        confidence = 0.75
        reason_codes.extend(["zone_still_strong", "continuation_supportive", "settling_window"])
        hold_until_unix = now_ts + 6 * 60
        message = "Hold here a bit longer — this zone still has enough continuation."
    elif (
        current_rating >= 50
        and tripless_minutes >= 20
        and stationary_minutes >= 14
        and (best_nearby is None or _safe_float(best_nearby.get("rating"), 0.0) < current_rating + MOVE_NEARBY_MIN_IMPROVEMENT)
    ):
        action = "micro_reposition"
        confidence = 0.62
        reason_codes.extend(["stationary_too_long", "zone_workable", "no_clear_nearby_edge"])
        hold_until_unix = now_ts + 5 * 60
        message = "Micro-reposition inside this zone; avoid a full jump right now."
    elif (
        best_nearby is not None
        and current_rating < 55
        and current_next_rating < 58
        and _safe_float(best_nearby.get("rating"), 0.0) >= current_rating + MOVE_NEARBY_MIN_IMPROVEMENT
        and not in_move_cooldown
        and recent_move_attempts < 3
        and _safe_float(best_nearby.get("distance_miles"), 999.0) <= 3.0
    ):
        action = "move_nearby"
        confidence = 0.72
        target_zone = dict(best_nearby)
        reason_codes.extend(["current_zone_weak", "nearby_materially_better", "cooldown_clear"])
        message = "Move to the nearby stronger zone with a material outlook edge."
    elif (
        recent_move_attempts >= 2
        and recent_saved_60 <= 0
        and dispatch_uncertainty >= 0.45
        and current_rating >= 48
    ):
        action = "wait_dispatch"
        confidence = 0.67
        reason_codes.extend(["recent_moves_failed", "dispatch_bottleneck_likely", "avoid_bounce"])
        hold_until_unix = now_ts + 8 * 60
        message = "Wait for dispatch — repeated moves haven’t converted and this area is still workable."
    elif in_move_cooldown:
        action = "wait_dispatch"
        confidence = 0.58
        reason_codes.extend(["move_cooldown_active", "anti_spam"])
        hold_until_unix = int(move_cooldown_until_unix)
        message = "Pause and wait for dispatch; recent movement guidance is still in cooldown."
    else:
        reason_codes.append("default_hold_bias")

    return {
        "action": action,
        "confidence": max(0.0, min(1.0, round(float(confidence), 3))),
        "message": message,
        "reason_codes": reason_codes,
        "current_zone": {
            "zone_id": current_zone_id,
            "zone_name": current_zone_name,
            "borough": current_borough,
            "rating": current_rating,
            "next_rating": current_next_rating,
            "market_saturation_penalty": current_saturation_penalty,
            "continuation_raw": current_continuation_raw,
        },
        "target_zone": target_zone,
        "tripless_minutes": round(tripless_minutes, 2),
        "stationary_minutes": round(stationary_minutes, 2),
        "movement_minutes": round(movement_minutes, 2),
        "recent_move_attempts_without_trip": int(recent_move_attempts),
        "recent_saved_trip_count": int(recent_saved_60),
        "dispatch_uncertainty": max(0.0, min(1.0, round(dispatch_uncertainty, 3))),
        "move_cooldown_until_unix": int(move_cooldown_until_unix) if move_cooldown_until_unix else None,
        "hold_until_unix": int(hold_until_unix) if hold_until_unix else None,
    }
