from __future__ import annotations

import json
import math
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from shapely.geometry import Point, shape
from pickup_recording_feature import pickup_log_not_voided_sql

MOVE_NEARBY_COOLDOWN_SECONDS = 11 * 60
MICRO_REPOSITION_COOLDOWN_SECONDS = 7 * 60
MOVE_NEARBY_MIN_IMPROVEMENT = 10.0
MOVE_NEARBY_STRONG_IMPROVEMENT = 13.0
RECENT_WINDOW_SECONDS = 2 * 3600

# Far-field reposition: when the whole local area is dead, only send the driver
# on a longer deadhead if the far zone is genuinely worth it — busy (blue+),
# clearly better than here, and reachable in a reasonable time.
FAR_FIELD_MIN_IMPROVEMENT = 15.0
FAR_FIELD_MAX_ETA_MIN = 35.0

# Earnings-rating -> demand bucket. "blue" (>=60) is the floor for a zone that's
# worth sitting in; sky blue (50-59) and below is a move zone. Thresholds match
# the frontend's colorFromRating so the words line up with the map colors.
BLUE_RATING = 60.0


def _bucket_name(rating: float) -> str:
    r = float(rating)
    if r >= 83:
        return "green"
    if r >= 75:
        return "purple"
    if r >= 68:
        return "indigo"
    if r >= 60:
        return "blue"
    if r >= 50:
        return "sky blue"
    if r >= 40:
        return "yellow"
    if r >= 30:
        return "orange"
    return "red"


# Elevated-risk TLC zones — the highest violent-crime areas per NYPD CompStat
# (South/Central Bronx, East Harlem, Brownsville / East New York). Used only
# to surface a DRIVER-SAFETY tip (raise the minimum rider rating); it never
# refuses the area and is based on crime statistics, not demographics.
SAFETY_ELEVATED_RISK_ZONE_IDS = frozenset({
    # Bronx (South / Central)
    47, 59, 60, 69, 78, 119, 126, 147, 159, 167, 168, 169, 212, 213, 247,
    # Brooklyn (Brownsville / East New York / Ocean Hill / Cypress Hills)
    35, 63, 76, 77, 177,
    # Manhattan (East Harlem)
    74, 75,
})
SAFETY_MIN_RIDER_RATING = 4.7

# A zone is a "low-trip trap" when it keeps pinging short, low-value trips
# that strand the driver: high short-trip penalty AND market saturation.
TRAP_SHORT_TRIP_PENALTY_MIN = 0.5
TRAP_SATURATION_PENALTY_MIN = 0.35

# TLC airport zones. Airports run a FIFO queue with mechanics the rating can't
# convey (hold your spot, EWR is NYC-bound only, LGA lot hours), so they get a
# dedicated overlay. Peak arrival windows worth queueing per research: weekday
# 9-11am and 5-8pm.
AIRPORT_ZONE_NAMES = {132: "JFK", 138: "LaGuardia", 1: "Newark"}

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


# --- Strategic-spot ("best location + best hours") integration ----------------
# Map the long-trip-hotspot clusters to their TLC zone so the guidance can point
# the driver to the exact spot inside a zone and tell them when it peaks.
_zone_hotspot_index_lock = threading.Lock()
_zone_hotspot_index_cache: Dict[str, Any] = {}


def build_zone_hotspot_index(
    hotspots: List[Dict[str, Any]],
    zones_geojson_path: Path,
    cache_key: str,
) -> Dict[int, List[Dict[str, Any]]]:
    """zone_id -> [clusters], cached by cache_key (rebuilds only when the POI
    set changes, so it auto-adapts to future zone/POI edits)."""
    with _zone_hotspot_index_lock:
        if _zone_hotspot_index_cache.get("key") == cache_key:
            return _zone_hotspot_index_cache.get("index") or {}
    zones = _load_zone_geometries(Path(zones_geojson_path))
    index: Dict[int, List[Dict[str, Any]]] = {}
    for h in hotspots or []:
        try:
            pt = Point(float(h["lng"]), float(h["lat"]))
        except Exception:
            continue
        zid: Optional[int] = None
        for zone in zones.values():
            geom = zone.get("geometry")
            if geom is not None and geom.covers(pt):
                zid = int(zone.get("zone_id"))
                break
        if zid is None:
            continue
        prime = ((h.get("dim_schedule") or {}).get("prime")) or []
        index.setdefault(zid, []).append({
            "label": h.get("label"),
            "lat": _safe_float(h.get("lat")),
            "lng": _safe_float(h.get("lng")),
            "best_hours": h.get("best_hours"),
            "address": h.get("address"),
            "prime_ranges": [list(r) for r in prime if isinstance(r, (list, tuple)) and len(r) == 2],
            "total_weight": _safe_float(h.get("total_weight"), 0.0),
        })
    for zid in index:
        index[zid].sort(key=lambda c: -c["total_weight"])
    with _zone_hotspot_index_lock:
        _zone_hotspot_index_cache.clear()
        _zone_hotspot_index_cache["key"] = cache_key
        _zone_hotspot_index_cache["index"] = index
    return index


def _hour_in_ranges(hour: int, ranges: List[List[int]]) -> bool:
    for r in ranges:
        if len(r) != 2:
            continue
        a, b = int(r[0]), int(r[1])
        if a <= b:
            if a <= hour < b:
                return True
        elif hour >= a or hour < b:  # wraps midnight
            return True
    return False


def zone_hotspot_hint(
    zone_id: Optional[int],
    arrival_hour: int,
    index: Dict[int, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    """Best strategic spot in the zone, flagged prime if arrival_hour is inside
    its window."""
    if zone_id is None:
        return None
    clusters = index.get(int(zone_id)) or []
    if not clusters:
        return None
    prime = [c for c in clusters if _hour_in_ranges(int(arrival_hour), c["prime_ranges"])]
    pick = (prime or clusters)[0]
    return {
        "label": pick["label"],
        "position": [pick["lat"], pick["lng"]],
        "best_hours": pick["best_hours"],
        "address": pick.get("address"),
        "prime_now": bool(prime),
    }


def load_driver_activity_snapshot(
    *,
    user_id: int,
    now_ts: int,
    current_lat: Optional[float],
    current_lng: Optional[float],
    db_query_one,
    db_query_all,
    current_zone_id: Optional[int] = None,
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
        f"""
        SELECT id, lat, lng, created_at
        FROM pickup_logs pl
        WHERE pl.user_id=?
          AND {pickup_log_not_voided_sql('pl')}
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (int(user_id),),
    )
    counts_row = db_query_one(
        f"""
        SELECT
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c30,
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c60,
          SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS c120
        FROM pickup_logs pl
        WHERE pl.user_id=?
          AND {pickup_log_not_voided_sql('pl')}
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
    recent_guidance_rows = db_query_all(
        """
        SELECT action, converted_to_trip, recommended_at, settled_at, moved_before_trip
        FROM assistant_guidance_outcomes
        WHERE user_id=?
          AND recommended_at >= ?
        ORDER BY recommended_at DESC, id DESC
        LIMIT 40
        """,
        (int(user_id), int(now_ts) - 5400),
    )
    state_row = db_query_one("SELECT * FROM driver_guidance_state WHERE user_id=? LIMIT 1", (int(user_id),))

    latest_trip_created_at = (latest_trip or {}).get("created_at")
    if latest_trip_created_at is not None:
        tripless_minutes = max(0.0, (float(now_ts) - _safe_float(latest_trip_created_at, float(now_ts))) / 60.0)
    else:
        baseline_ts = _safe_float((guard_row or {}).get("movement_streak_started_at"), 0.0)
        if baseline_ts <= 0:
            baseline_ts = _safe_float((guard_row or {}).get("last_meaningful_motion_at"), 0.0)
        if baseline_ts <= 0:
            baseline_ts = _safe_float((presence_row or {}).get("updated_at"), float(now_ts))
        tripless_minutes = max(0.0, (float(now_ts) - float(baseline_ts)) / 60.0)
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

    recent_guidance_move_attempts_without_trip = 0
    for row in recent_guidance_rows:
        action = str((row or {}).get("action") or "").strip().lower()
        if action not in {"move_nearby", "micro_reposition"}:
            continue
        converted = (row or {}).get("converted_to_trip")
        if converted in (1, True):
            break
        recent_guidance_move_attempts_without_trip += 1

    # Per-zone dwell: minutes since the driver was last seen in a DIFFERENT
    # zone (i.e. how long they've been in the current one). Robust to gaps and
    # uncapped within the 2h window. No schema change — reuses the outcomes log.
    zone_dwell_minutes = 0.0
    if current_zone_id is not None:
        boundary_row = db_query_one(
            "SELECT MAX(recommended_at) AS boundary FROM assistant_guidance_outcomes "
            "WHERE user_id=? AND recommended_at >= ? "
            "AND source_zone_id IS NOT NULL AND source_zone_id != ?",
            (int(user_id), int(now_ts) - 7200, int(current_zone_id)),
        )
        boundary_ts = _safe_int((boundary_row or {}).get("boundary"), 0)
        if boundary_ts <= 0:
            earliest_row = db_query_one(
                "SELECT MIN(recommended_at) AS earliest FROM assistant_guidance_outcomes "
                "WHERE user_id=? AND recommended_at >= ? AND source_zone_id = ?",
                (int(user_id), int(now_ts) - 7200, int(current_zone_id)),
            )
            boundary_ts = _safe_int((earliest_row or {}).get("earliest"), 0)
        if boundary_ts > 0:
            zone_dwell_minutes = max(0.0, (float(now_ts) - float(boundary_ts)) / 60.0)

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
    if recent_guidance_move_attempts_without_trip >= 2:
        uncertainty += 0.1

    presence_updated = _safe_int((presence_row or {}).get("updated_at"), 0)
    current_presence_stale = (now_ts - presence_updated) > 300 if presence_updated > 0 else True

    state_attempts = _safe_int((state_row or {}).get("recent_move_attempts_without_trip"), 0)
    return {
        "tripless_minutes": round(tripless_minutes, 2),
        "stationary_minutes": round(stationary_minutes, 2),
        "movement_minutes": round(movement_minutes, 2),
        "zone_dwell_minutes": round(zone_dwell_minutes, 2),
        "moved_since_last_saved_trip": bool(moved_since_last_saved_trip),
        "recent_saved_trip_count_30m": int(recent_saved_trip_count_30m),
        "recent_saved_trip_count_60m": int(recent_saved_trip_count_60m),
        "recent_saved_trip_count_120m": int(recent_saved_trip_count_120m),
        "recent_move_attempts_without_trip": max(state_attempts, recent_guidance_move_attempts_without_trip),
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
    _ = user_id, current_lat, current_lng, mode_flags, assistant_outlook_bucket
    # Overnight (9pm-5am) carries ~20-25x the violent-crime rate of rush hour
    # (NYPD/Vital City), and the documented driver-specific threat is the
    # "tap-and-snatch" phone/payment-app robbery by fake riders.
    try:
        _frame_hour = int(str(frame_time)[11:13])
    except Exception:
        _frame_hour = 12
    overnight = _frame_hour >= 21 or _frame_hour < 5
    tripless_minutes = _safe_float(activity_snapshot.get("tripless_minutes"))
    stationary_minutes = _safe_float(activity_snapshot.get("stationary_minutes"))
    zone_dwell_minutes = _safe_float(activity_snapshot.get("zone_dwell_minutes"))
    movement_minutes = _safe_float(activity_snapshot.get("movement_minutes"))
    dispatch_uncertainty = _safe_float(activity_snapshot.get("dispatch_uncertainty"), 0.3)
    recent_move_attempts = _safe_int(activity_snapshot.get("recent_move_attempts_without_trip"), 0)
    recent_saved_60 = _safe_int(activity_snapshot.get("recent_saved_trip_count_60m"), 0)
    moved_since_last_saved_trip = bool(activity_snapshot.get("moved_since_last_saved_trip"))
    state = activity_snapshot.get("guidance_state") or {}

    current_zone = zone_context.get("current_zone") or {}
    nearby_candidates = zone_context.get("nearby_candidates") or []
    best_nearby = nearby_candidates[0] if nearby_candidates else None
    # Opportunity cost: a farther move must clear a higher bar (more driving,
    # more earnings given up en route). best_nearby.rating is already the
    # arrival-time score, so this only adds the travel cost on top.
    best_nearby_eta = _safe_float(best_nearby.get("eta_minutes"), 0.0) if best_nearby else 0.0
    # The longer the driver has sat in this zone without it producing, the
    # lower the bar a candidate must clear — the engine monitors dwell and
    # gets more willing to move them on.
    dwell_discount = min(8.0, max(0.0, zone_dwell_minutes - 15.0) * 0.4)
    move_improvement_required = max(
        4.0, MOVE_NEARBY_STRONG_IMPROVEMENT + min(10.0, best_nearby_eta * 0.6) - dwell_discount
    )

    current_rating = _safe_float(current_zone.get("rating"), 0.0)
    current_next_rating = _safe_float(current_zone.get("next_rating"), current_rating)
    current_saturation_penalty = _safe_float(current_zone.get("market_saturation_penalty"), 0.0)
    current_continuation_raw = _safe_float(current_zone.get("continuation_raw"), 0.0)
    current_bucket = current_zone.get("bucket")
    current_color = current_zone.get("color")
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

    # === Blue-floor rule ===========================================
    # Sky blue (50-59) and below is NOT a sit zone: the driver belongs in a
    # blue+ (>=60) zone. Below blue, move to a zone that will be blue+ when the
    # driver ARRIVES; only hold if THIS zone is itself about to rise to blue+
    # and nothing reachable is already stronger. Whenever the current zone is
    # improving, say so, so the choice is informed. Anti-churn still applies:
    # we won't push a move during cooldown or after repeated failed moves.
    below_blue = current_rating < BLUE_RATING
    current_will_improve = below_blue and current_next_rating >= BLUE_RATING
    best_nearby_arrival = _safe_float(best_nearby.get("rating"), 0.0) if best_nearby else 0.0
    best_nearby_dist = _safe_float(best_nearby.get("distance_miles"), 999.0) if best_nearby else 999.0
    best_nearby_name = (best_nearby or {}).get("zone_name") or "the nearby zone"
    nearby_blue_on_arrival = (
        best_nearby is not None and best_nearby_arrival >= BLUE_RATING and best_nearby_dist <= 3.0
    )
    can_move = (not in_move_cooldown) and recent_move_attempts < 2
    improvement_note: Optional[str] = None
    blue_rule_applied = False

    if below_blue:
        # Current climbing ABOVE the best reachable zone -> it becomes the better
        # spot, so hold for it instead of chasing a now-weaker move.
        climbs_above_nearby = (
            current_will_improve and current_next_rating > best_nearby_arrival + 2.0
        )
        if nearby_blue_on_arrival and can_move and not climbs_above_nearby:
            action = "move_nearby"
            confidence = 0.74
            target_zone = dict(best_nearby)
            hold_until_unix = None
            reason_codes = ["below_blue", "target_blue_on_arrival", "blue_floor_move"]
            message = f"Move to {best_nearby_name} — it'll be busier when you arrive."
            if current_will_improve:
                improvement_note = (
                    f"This area's picking up too, but {best_nearby_name} "
                    f"will be better when you get there."
                )
            blue_rule_applied = True
        elif current_will_improve:
            action = "wait_dispatch"
            confidence = 0.70
            target_zone = None
            hold_until_unix = now_ts + 6 * 60
            reason_codes = ["below_blue_but_improving", "hold_for_rise"]
            message = "Stay put — this area is about to pick up."
            if nearby_blue_on_arrival:
                # Holding because this zone out-climbs the best reachable move.
                improvement_note = (
                    "This area's about to get busier in a few minutes — "
                    "better than moving, so stay."
                )
            else:
                improvement_note = (
                    "This area's about to get busier in a few minutes — sit tight."
                )
            blue_rule_applied = True
        elif (
            can_move
            and best_nearby is not None
            and best_nearby_dist <= 2.5
            and best_nearby_arrival >= current_rating + MOVE_NEARBY_MIN_IMPROVEMENT
        ):
            action = "move_nearby"
            confidence = 0.64
            target_zone = dict(best_nearby)
            hold_until_unix = None
            reason_codes = ["below_blue_no_blue_anywhere", "move_to_better"]
            message = f"Weak here and nothing's blue nearby — {best_nearby_name} is the better option."
            blue_rule_applied = True
        # else: cooldown / churn / nothing better -> fall through to base logic.

    if blue_rule_applied:
        pass
    elif current_rating >= 64 and current_next_rating >= (current_rating - 4) and current_continuation_raw >= 0.45 and settling_window:
        action = "hold"
        confidence = 0.75
        reason_codes.extend(["zone_still_strong", "continuation_supportive", "settling_window"])
        hold_until_unix = now_ts + 6 * 60
        message = "Hold here a bit longer — this zone still has enough continuation."
    elif (
        current_rating >= 50
        and moved_since_last_saved_trip
        and recent_move_attempts >= 1
        and dispatch_uncertainty >= 0.5
        and recent_saved_60 <= 0
    ):
        action = "wait_dispatch"
        confidence = 0.7
        reason_codes.extend(["recent_movement_no_conversion", "dispatch_bottleneck_likely", "avoid_repeat_reposition"])
        hold_until_unix = now_ts + 8 * 60
        message = "You already moved recently. Wait for dispatch instead of churning position again."
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
        and _safe_float(best_nearby.get("rating"), 0.0) >= current_rating + move_improvement_required
        and not in_move_cooldown
        and recent_move_attempts < 3
        and _safe_float(best_nearby.get("distance_miles"), 999.0) <= 2.5
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

    # --- Far-field reposition: when we'd otherwise just sit in a dead area and
    # nothing within a few miles is any better, point the driver at where the
    # demand actually is — the best strong zone within a longer, worthwhile
    # drive. This is the "smarter than the driver" move: it sees the whole city.
    far_reposition = False
    if (
        below_blue
        and not current_will_improve
        and action in {"hold", "wait_dispatch"}
        and not nearby_blue_on_arrival
        and not in_move_cooldown
        and recent_move_attempts < 3
    ):
        far_list = zone_context.get("far_candidates") or []
        best_far = far_list[0] if far_list else None
        if best_far is not None:
            far_rating = _safe_float(best_far.get("rating"), 0.0)
            far_eta = _safe_float(best_far.get("eta_minutes"), 0.0)
            if (
                far_rating >= BLUE_RATING
                and far_rating >= current_rating + FAR_FIELD_MIN_IMPROVEMENT
                and 0.0 < far_eta <= FAR_FIELD_MAX_ETA_MIN
            ):
                action = "move_nearby"
                confidence = 0.66
                target_zone = dict(best_far)
                hold_until_unix = None
                far_reposition = True
                improvement_note = None
                reason_codes.append("far_field_reposition")
                message = f"Slow all around here — head for {best_far.get('zone_name')}."

    # --- Safety overlay: elevated-risk zone -> raise the rider-rating filter,
    # and overnight add the phone-snatch defense (the documented driver threat).
    safety_elevated_risk = current_zone_id in SAFETY_ELEVATED_RISK_ZONE_IDS
    safety_advice: Optional[str] = None
    if safety_elevated_risk:
        safety_advice = (
            f"Higher-risk area — only take {SAFETY_MIN_RIDER_RATING:g}+ riders here."
        )
        if overnight:
            safety_advice += " Late-night: keep your phone mounted, never hand it to a rider."
        reason_codes.append("elevated_risk_zone")
        if overnight:
            reason_codes.append("overnight_high_risk")

    # --- Trap escape: stuck in a short-trip trap while being told to move.
    current_short_trip_penalty = _safe_float(current_zone.get("short_trip_penalty"), 0.0)
    trap_zone = (
        current_short_trip_penalty >= TRAP_SHORT_TRIP_PENALTY_MIN
        and current_saturation_penalty >= TRAP_SATURATION_PENALTY_MIN
    )
    offline_until_arrival = bool(
        trap_zone and action in {"move_nearby", "micro_reposition"} and target_zone
    )
    trap_advice: Optional[str] = None
    if offline_until_arrival:
        trap_advice = (
            f"This area keeps sending short, cheap trips — go offline until you reach "
            f"{target_zone.get('zone_name')}."
        )
        reason_codes.append("low_trip_trap_escape")
    elif trap_zone:
        reason_codes.append("low_trip_trap")

    # --- Airport overlay: FIFO queue mechanics the rating can't convey.
    airport_advice: Optional[str] = None
    _airport = AIRPORT_ZONE_NAMES.get(current_zone_id)
    if _airport:
        reason_codes.append("airport_zone")
        if _airport == "LaGuardia" and (1 <= _frame_hour < 6):
            airport_advice = "LaGuardia's rideshare lot is closed now (open 6am–1am)."
        else:
            peak = (9 <= _frame_hour < 11) or (17 <= _frame_hour < 20)
            lead = (
                "Peak arrival hours — the queue should move."
                if peak
                else "Off-peak for arrivals — only queue if the lot's short, else work the city."
            )
            tail = "Hold your FIFO spot: don't go offline or decline 2+ in a row, or you lose your place."
            if _airport == "Newark":
                tail = "Take NYC-bound trips only (NJ rule). " + tail
            airport_advice = f"{lead} {tail}"

    # Fold the overlay tips into the headline message.
    message = " ".join(
        [part for part in (message, improvement_note, trap_advice, airport_advice, safety_advice) if part]
    )

    return {
        "action": action,
        "confidence": max(0.0, min(1.0, round(float(confidence), 3))),
        "message": message,
        "reason_codes": reason_codes,
        "safety_elevated_risk": bool(safety_elevated_risk),
        "safety_overnight": bool(overnight),
        "safety_advice": safety_advice,
        "safety_min_rider_rating": SAFETY_MIN_RIDER_RATING if safety_elevated_risk else None,
        "trap_zone": bool(trap_zone),
        "offline_until_arrival": bool(offline_until_arrival),
        "trap_advice": trap_advice,
        "airport_advice": airport_advice,
        "below_blue": bool(below_blue),
        "current_will_improve": bool(current_will_improve),
        "improvement_note": improvement_note,
        "far_reposition": bool(far_reposition),
        "nearby_candidates": nearby_candidates[:5],
        "current_zone": {
            "zone_id": current_zone_id,
            "zone_name": current_zone_name,
            "borough": current_borough,
            "rating": current_rating,
            "bucket": current_bucket,
            "color": current_color,
            "next_rating": current_next_rating,
            "market_saturation_penalty": current_saturation_penalty,
            "continuation_raw": current_continuation_raw,
        },
        "target_zone": target_zone,
        "tripless_minutes": round(tripless_minutes, 2),
        "stationary_minutes": round(stationary_minutes, 2),
        "movement_minutes": round(movement_minutes, 2),
        "zone_dwell_minutes": round(zone_dwell_minutes, 2),
        "recent_move_attempts_without_trip": int(recent_move_attempts),
        "recent_saved_trip_count": int(recent_saved_60),
        "dispatch_uncertainty": max(0.0, min(1.0, round(dispatch_uncertainty, 3))),
        "move_cooldown_until_unix": int(move_cooldown_until_unix) if move_cooldown_until_unix else None,
        "hold_until_unix": int(hold_until_unix) if hold_until_unix else None,
    }
