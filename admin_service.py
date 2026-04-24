from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from core import DB_BACKEND, _db_query_all, _db_query_one
from pickup_recording_feature import pickup_log_not_voided_sql


def _flag_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return int(value) == 1


def _flag_to_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return int(value) == 1


def _outcome_status(converted_to_trip: Optional[bool]) -> str:
    if converted_to_trip is None:
        return "pending"
    return "converted" if converted_to_trip else "not_converted"


def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


def _build_recent_filter_clause_and_params(
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    user_id: Optional[int] = None,
    outcome_status: Optional[str] = None,
    since_seconds: Optional[int] = None,
    time_column: str = "recommended_at",
    cluster_column: str = "cluster_id",
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if zone_id is not None:
        clauses.append("zone_id = ?")
        params.append(int(zone_id))

    if cluster_id is not None:
        clauses.append(f"{cluster_column} = ?")
        params.append(str(cluster_id))

    if user_id is not None:
        clauses.append("user_id = ?")
        params.append(int(user_id))

    if outcome_status is not None:
        normalized = str(outcome_status).strip().lower()
        if normalized == "pending":
            clauses.append("converted_to_trip IS NULL")
        elif normalized == "converted":
            if DB_BACKEND == "postgres":
                clauses.append("converted_to_trip = TRUE")
            else:
                clauses.append("CAST(converted_to_trip AS INTEGER) = 1")
        elif normalized == "not_converted":
            if DB_BACKEND == "postgres":
                clauses.append("converted_to_trip = FALSE")
            else:
                clauses.append("CAST(converted_to_trip AS INTEGER) = 0")
        else:
            raise ValueError("outcome_status must be one of: pending, converted, not_converted")

    if since_seconds is not None:
        clamped_since_seconds = max(1, min(2_592_000, int(since_seconds)))
        cutoff = int(time.time()) - clamped_since_seconds
        if DB_BACKEND == "postgres":
            clauses.append(f"{time_column} >= to_timestamp(?)")
        else:
            clauses.append(f"{time_column} >= ?")
        params.append(cutoff)

    return clauses, params


def _clamp_since_seconds(since_seconds: Optional[int]) -> Optional[int]:
    if since_seconds is None:
        return None
    return max(1, min(2_592_000, int(since_seconds)))


def _normalize_cluster_id(cluster_id: Optional[str]) -> Optional[str]:
    if cluster_id is None:
        return None
    normalized = str(cluster_id).strip()
    return normalized or None


def _build_bin_filter_clause_and_params(
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    since_seconds: Optional[int] = None,
    time_column: str = "bin_time",
    cluster_column: str = "cluster_id",
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if zone_id is not None:
        clauses.append("zone_id = ?")
        params.append(int(zone_id))

    if cluster_id is not None:
        clauses.append(f"{cluster_column} = ?")
        params.append(str(cluster_id))

    clamped_since_seconds = _clamp_since_seconds(since_seconds)
    if clamped_since_seconds is not None:
        cutoff = int(time.time()) - clamped_since_seconds
        if DB_BACKEND == "postgres":
            clauses.append(f"{time_column} >= to_timestamp(?)")
        else:
            clauses.append(f"{time_column} >= ?")
        params.append(cutoff)

    return clauses, params


# Table names go into an f-string with no parameterization because SQL does
# not permit binding a table name as a parameter. Gate the interpolation
# behind a hardcoded whitelist so a future refactor cannot turn this into
# SQL injection. All current call sites pass literals from this set.
_COUNTABLE_TABLES = frozenset({
    "users",
    "presence",
    "events",
    "pickup_logs",
    "chat_messages",
    "private_chat_messages",
    "hotspot_experiment_bins",
    "micro_hotspot_experiment_bins",
    "recommendation_outcomes",
    "micro_recommendation_outcomes",
    "driver_daily_stats",
    "driver_work_state",
    "leaderboard_badges_current",
    "paddle_webhook_events",
})


def _safe_count(table: str) -> Optional[int]:
    if table not in _COUNTABLE_TABLES:
        return None
    try:
        row = _db_query_one(f"SELECT COUNT(*) AS c FROM {table}")
        return int(row["c"]) if row else 0
    except Exception:
        return None


def _recent_count(table: str, window_seconds: int = 86400) -> int:
    if table not in _COUNTABLE_TABLES:
        return 0
    cutoff = int(time.time()) - int(window_seconds)
    try:
        if DB_BACKEND == "postgres":
            row = _db_query_one(f"SELECT COUNT(*) AS c FROM {table} WHERE created_at >= to_timestamp(?)", (cutoff,))
        else:
            row = _db_query_one(f"SELECT COUNT(*) AS c FROM {table} WHERE created_at >= ?", (cutoff,))
        return int(row["c"]) if row else 0
    except Exception:
        return 0


def _recent_police_reports_count(window_seconds: int = 86400) -> int:
    cutoff = int(time.time()) - int(window_seconds)
    try:
        row = _db_query_one(
            """
            SELECT COUNT(*) AS c
            FROM events
            WHERE lower(type) = 'police' AND created_at >= ?
            """,
            (cutoff,),
        )
        return int(row["c"]) if row else 0
    except Exception:
        return 0


def _recent_active_pickup_logs_count(window_seconds: int = 86400) -> int:
    cutoff = int(time.time()) - int(window_seconds)
    try:
        row = _db_query_one(
            f"""
            SELECT COUNT(*) AS c
            FROM pickup_logs pl
            WHERE pl.created_at >= ?
              AND {pickup_log_not_voided_sql('pl')}
            """,
            (cutoff,),
        )
        return int(row["c"]) if row else 0
    except Exception:
        return 0


def _recent_voided_pickup_logs_count(window_seconds: int = 86400) -> int:
    cutoff = int(time.time()) - int(window_seconds)
    try:
        row = _db_query_one(
            f"""
            SELECT COUNT(*) AS c
            FROM pickup_logs pl
            WHERE pl.created_at >= ?
              AND NOT ({pickup_log_not_voided_sql('pl')})
            """,
            (cutoff,),
        )
        return int(row["c"]) if row else 0
    except Exception:
        return 0


def _admin_online_presence_counts(max_age_sec: int = 300) -> Dict[str, int]:
    cutoff = int(time.time()) - int(max_age_sec)
    try:
        if DB_BACKEND == "postgres":
            ghosted_expr = "CASE WHEN COALESCE(u.ghost_mode, FALSE) = TRUE THEN 1 ELSE 0 END"
        else:
            ghosted_expr = "CASE WHEN COALESCE(CAST(u.ghost_mode AS INTEGER), 0) = 1 THEN 1 ELSE 0 END"

        row = _db_query_one(
            f"""
            SELECT
              COUNT(*) AS online_count,
              COALESCE(SUM({ghosted_expr}), 0) AS ghosted_count
            FROM presence p
            LEFT JOIN users u ON u.id = p.user_id
            WHERE p.updated_at >= ?
            """,
            (cutoff,),
        )
        return {
            "online_users": int(row["online_count"] or 0) if row else 0,
            "ghosted_online_users": int(row["ghosted_count"] or 0) if row else 0,
        }
    except Exception:
        return {"online_users": 0, "ghosted_online_users": 0}


def _frames_info() -> Dict[str, Any]:
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    frames_dir = Path(os.environ.get("FRAMES_DIR", str(data_dir / "frames")))
    timeline_path = frames_dir / "timeline.json"
    timeline_ready = timeline_path.exists() and timeline_path.stat().st_size > 0 if timeline_path.exists() else False
    frame_count = 0
    if frames_dir.exists():
        frame_count = len(list(frames_dir.glob("*.geojson")))
    return {
        "timeline_ready": timeline_ready,
        "frame_count": frame_count,
        "frames_dir": str(frames_dir),
        "data_dir": str(data_dir),
    }


def _leaderboard_status() -> Dict[str, Any]:
    try:
        from leaderboard_service import get_current_badges_for_user  # lazy import

        _ = get_current_badges_for_user
        return {"available": True}
    except Exception as exc:
        return {"available": False, "detail": str(exc)}


def get_admin_summary() -> Dict[str, Any]:
    total_users = _safe_count("users") or 0

    try:
        if DB_BACKEND == "postgres":
            row = _db_query_one(
                "SELECT COUNT(*) AS c FROM users WHERE COALESCE(is_admin, FALSE) = TRUE"
            )
        else:
            row = _db_query_one(
                "SELECT COUNT(*) AS c FROM users WHERE COALESCE(CAST(is_admin AS INTEGER), 0) = 1"
            )
        admin_users = int(row["c"]) if row else 0
    except Exception:
        admin_users = 0

    presence_counts = _admin_online_presence_counts(max_age_sec=300)
    online_users = int(presence_counts["online_users"])
    ghosted_online_users = int(presence_counts["ghosted_online_users"])

    police_reports_recent_count = _recent_police_reports_count()
    pickup_logs_recent_count = _recent_active_pickup_logs_count()
    pickup_logs_voided_recent_count = _recent_voided_pickup_logs_count()

    frames = _frames_info()
    return {
        "total_users": total_users,
        "admin_users": admin_users,
        "admins_count": admin_users,
        "online_users": online_users,
        "ghosted_online_users": ghosted_online_users,
        "police_reports_recent_count": police_reports_recent_count,
        "police_reports_count": police_reports_recent_count,
        "pickup_logs_recent_count": pickup_logs_recent_count,
        "pickup_logs_count": pickup_logs_recent_count,
        "pickup_logs_voided_recent_count": pickup_logs_voided_recent_count,
        "timeline_ready": frames["timeline_ready"],
        "frame_count": frames["frame_count"],
        "leaderboard_status": _leaderboard_status(),
        "backend_status": "ok",
    }


def get_admin_users(limit: int = 500) -> List[Dict[str, Any]]:
    rows = _db_query_all(
        """
        SELECT id, email, display_name, is_admin, is_disabled, is_suspended, ghost_mode, avatar_url, created_at
        FROM users
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    items: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        items.append(
            {
                "id": int(row["id"]),
                "email": row.get("email"),
                "display_name": row.get("display_name"),
                "is_admin": _flag_to_bool(row.get("is_admin")),
                "is_disabled": _flag_to_bool(row.get("is_disabled")),
                "is_suspended": _flag_to_bool(row.get("is_suspended")),
                "ghost_mode": _flag_to_bool(row.get("ghost_mode")),
                "avatar_url": row.get("avatar_url"),
                "created_at": _to_iso(row.get("created_at")),
            }
        )
    return items


def get_admin_live(limit: int = 1000) -> List[Dict[str, Any]]:
    if DB_BACKEND == "postgres":
        sql = """
        SELECT p.user_id, u.display_name, p.lat, p.lng, p.heading, p.accuracy, u.ghost_mode, p.updated_at
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        ORDER BY p.updated_at DESC
        LIMIT ?
        """
    else:
        sql = """
        SELECT p.user_id, u.display_name, p.lat, p.lng, p.heading, p.accuracy, u.ghost_mode, p.updated_at
        FROM presence p
        LEFT JOIN users u ON u.id = p.user_id
        ORDER BY p.updated_at DESC
        LIMIT ?
        """

    rows = _db_query_all(sql, (int(limit),))

    badge_map: Dict[int, Dict[str, Any]] = {}
    try:
        from leaderboard_service import get_best_current_badges_for_users

        user_ids = [int(dict(r)["user_id"]) for r in rows]
        badge_map = get_best_current_badges_for_users(user_ids)
    except Exception:
        badge_map = {}

    items: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        badge = badge_map.get(int(row["user_id"]), {}) if badge_map else {}
        items.append(
            {
                "user_id": int(row["user_id"]),
                "display_name": row.get("display_name"),
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "heading": float(row["heading"]) if row.get("heading") is not None else None,
                "accuracy": float(row["accuracy"]) if row.get("accuracy") is not None else None,
                "ghost_mode": _flag_to_bool(row.get("ghost_mode")),
                "updated_at": _to_iso(row.get("updated_at")),
                "leaderboard_badge_code": badge.get("badge_code"),
                "leaderboard_has_crown": bool(badge.get("has_crown")) if badge else None,
            }
        )
    return items


def get_admin_police_reports(limit: int = 500) -> List[Dict[str, Any]]:
    rows = _db_query_all(
        """
        SELECT id, user_id, lat, lng, zone_id, created_at, expires_at
        FROM events
        WHERE lower(type) = 'police'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "user_id": int(dict(r)["user_id"]) if dict(r).get("user_id") is not None else None,
            "lat": float(dict(r)["lat"]),
            "lng": float(dict(r)["lng"]),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "created_at": _to_iso(dict(r).get("created_at")),
            "expires_at": _to_iso(dict(r).get("expires_at")),
        }
        for r in rows
    ]


def get_admin_pickup_logs(limit: int = 500) -> List[Dict[str, Any]]:
    rows = _db_query_all(
        f"""
        SELECT pl.id, pl.user_id, pl.zone_id, pl.zone_name, pl.borough, pl.lat, pl.lng, pl.frame_time, pl.created_at, pl.guard_reason
        FROM pickup_logs pl
        WHERE {pickup_log_not_voided_sql('pl')}
        ORDER BY pl.created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "user_id": int(dict(r)["user_id"]) if dict(r).get("user_id") is not None else None,
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "zone_name": dict(r).get("zone_name"),
            "borough": dict(r).get("borough"),
            "lat": float(dict(r)["lat"]),
            "lng": float(dict(r)["lng"]),
            "frame_time": dict(r).get("frame_time"),
            "created_at": _to_iso(dict(r).get("created_at")),
            "guard_reason": dict(r).get("guard_reason"),
        }
        for r in rows
    ]


def get_admin_hotspot_experiment_bins(
    limit: int = 200,
    zone_id: Optional[int] = None,
    since_seconds: Optional[int] = None,
    recommended_only: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    where_clauses: list[str] = []
    params: list[Any] = []

    if zone_id is not None:
        where_clauses.append("zone_id = ?")
        params.append(int(zone_id))

    if since_seconds is not None:
        clamped_since_seconds = max(1, min(2_592_000, int(since_seconds)))
        cutoff = int(time.time()) - clamped_since_seconds
        if DB_BACKEND == "postgres":
            where_clauses.append("bin_time >= to_timestamp(?)")
        else:
            where_clauses.append("bin_time >= ?")
        params.append(cutoff)

    if recommended_only is True:
        if DB_BACKEND == "postgres":
            where_clauses.append("recommended = TRUE")
        else:
            where_clauses.append("CAST(recommended AS INTEGER) = 1")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = _db_query_all(
        f"""
        SELECT
            id, bin_time, zone_id, final_score, confidence,
            historical_component, live_component, same_timeslot_component,
            long_run_historical_component, recent_shape_component,
            outcome_modifier, quality_modifier, saturation_modifier,
            hotspot_limit_used, density_penalty, weighted_trip_count,
            unique_driver_count, recommended
        FROM hotspot_experiment_bins
        {where_sql}
        ORDER BY bin_time DESC, id DESC
        LIMIT ?
        """,
        tuple(params + [clamped_limit]),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "bin_time": dict(r).get("bin_time"),
            "bin_time_iso": _to_iso(dict(r).get("bin_time")),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "final_score": float(dict(r)["final_score"]) if dict(r).get("final_score") is not None else None,
            "confidence": float(dict(r)["confidence"]) if dict(r).get("confidence") is not None else None,
            "historical_component": float(dict(r)["historical_component"]) if dict(r).get("historical_component") is not None else None,
            "live_component": float(dict(r)["live_component"]) if dict(r).get("live_component") is not None else None,
            "same_timeslot_component": float(dict(r)["same_timeslot_component"]) if dict(r).get("same_timeslot_component") is not None else None,
            "long_run_historical_component": float(dict(r)["long_run_historical_component"]) if dict(r).get("long_run_historical_component") is not None else None,
            "recent_shape_component": float(dict(r)["recent_shape_component"]) if dict(r).get("recent_shape_component") is not None else None,
            "outcome_modifier": float(dict(r)["outcome_modifier"]) if dict(r).get("outcome_modifier") is not None else None,
            "quality_modifier": float(dict(r)["quality_modifier"]) if dict(r).get("quality_modifier") is not None else None,
            "saturation_modifier": float(dict(r)["saturation_modifier"]) if dict(r).get("saturation_modifier") is not None else None,
            "hotspot_limit_used": int(dict(r)["hotspot_limit_used"]) if dict(r).get("hotspot_limit_used") is not None else None,
            "density_penalty": float(dict(r)["density_penalty"]) if dict(r).get("density_penalty") is not None else None,
            "weighted_trip_count": float(dict(r)["weighted_trip_count"]) if dict(r).get("weighted_trip_count") is not None else None,
            "unique_driver_count": int(dict(r)["unique_driver_count"]) if dict(r).get("unique_driver_count") is not None else None,
            "recommended": _flag_to_bool(dict(r).get("recommended")),
        }
        for r in rows
    ]


def get_admin_micro_hotspot_experiment_bins(
    limit: int = 200,
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    since_seconds: Optional[int] = None,
    recommended_only: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    where_clauses: list[str] = []
    params: list[Any] = []

    if zone_id is not None:
        where_clauses.append("zone_id = ?")
        params.append(int(zone_id))

    if cluster_id is not None:
        where_clauses.append("cluster_id = ?")
        params.append(str(cluster_id))

    if since_seconds is not None:
        clamped_since_seconds = max(1, min(2_592_000, int(since_seconds)))
        cutoff = int(time.time()) - clamped_since_seconds
        if DB_BACKEND == "postgres":
            where_clauses.append("bin_time >= to_timestamp(?)")
        else:
            where_clauses.append("bin_time >= ?")
        params.append(cutoff)

    if recommended_only is True:
        if DB_BACKEND == "postgres":
            where_clauses.append("recommended = TRUE")
        else:
            where_clauses.append("CAST(recommended AS INTEGER) = 1")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = _db_query_all(
        f"""
        SELECT
            id, bin_time, zone_id, cluster_id, final_score, confidence,
            weighted_trip_count, unique_driver_count, crowding_penalty,
            center_lat, center_lng, radius_m, intensity,
            baseline_component, live_component, same_timeslot_component,
            eta_alignment, recommended
        FROM micro_hotspot_experiment_bins
        {where_sql}
        ORDER BY bin_time DESC, id DESC
        LIMIT ?
        """,
        tuple(params + [clamped_limit]),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "bin_time": dict(r).get("bin_time"),
            "bin_time_iso": _to_iso(dict(r).get("bin_time")),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "cluster_id": dict(r).get("cluster_id"),
            "final_score": float(dict(r)["final_score"]) if dict(r).get("final_score") is not None else None,
            "confidence": float(dict(r)["confidence"]) if dict(r).get("confidence") is not None else None,
            "weighted_trip_count": float(dict(r)["weighted_trip_count"]) if dict(r).get("weighted_trip_count") is not None else None,
            "unique_driver_count": int(dict(r)["unique_driver_count"]) if dict(r).get("unique_driver_count") is not None else None,
            "crowding_penalty": float(dict(r)["crowding_penalty"]) if dict(r).get("crowding_penalty") is not None else None,
            "center_lat": float(dict(r)["center_lat"]) if dict(r).get("center_lat") is not None else None,
            "center_lng": float(dict(r)["center_lng"]) if dict(r).get("center_lng") is not None else None,
            "radius_m": float(dict(r)["radius_m"]) if dict(r).get("radius_m") is not None else None,
            "intensity": float(dict(r)["intensity"]) if dict(r).get("intensity") is not None else None,
            "baseline_component": float(dict(r)["baseline_component"]) if dict(r).get("baseline_component") is not None else None,
            "live_component": float(dict(r)["live_component"]) if dict(r).get("live_component") is not None else None,
            "same_timeslot_component": float(dict(r)["same_timeslot_component"]) if dict(r).get("same_timeslot_component") is not None else None,
            "eta_alignment": float(dict(r)["eta_alignment"]) if dict(r).get("eta_alignment") is not None else None,
            "recommended": _flag_to_bool(dict(r).get("recommended")),
        }
        for r in rows
    ]


def get_admin_recommendation_outcomes(
    limit: int = 200,
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    user_id: Optional[int] = None,
    outcome_status: Optional[str] = None,
    since_seconds: Optional[int] = None,
) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    where_clauses, params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=cluster_id,
        user_id=user_id,
        outcome_status=outcome_status,
        since_seconds=since_seconds,
        time_column="recommended_at",
        cluster_column="cluster_id",
    )
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = _db_query_all(
        f"""
        SELECT
            id, user_id, recommended_at, zone_id, cluster_id,
            hotspot_center_lat, hotspot_center_lng, score, confidence,
            converted_to_trip, minutes_to_trip, distance_to_recommendation_miles
        FROM recommendation_outcomes
        {where_sql}
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        tuple(params + [clamped_limit]),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "user_id": int(dict(r)["user_id"]) if dict(r).get("user_id") is not None else None,
            "recommended_at": dict(r).get("recommended_at"),
            "recommended_at_iso": _to_iso(dict(r).get("recommended_at")),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "cluster_id": dict(r).get("cluster_id"),
            "hotspot_center_lat": float(dict(r)["hotspot_center_lat"]) if dict(r).get("hotspot_center_lat") is not None else None,
            "hotspot_center_lng": float(dict(r)["hotspot_center_lng"]) if dict(r).get("hotspot_center_lng") is not None else None,
            "score": float(dict(r)["score"]) if dict(r).get("score") is not None else None,
            "confidence": float(dict(r)["confidence"]) if dict(r).get("confidence") is not None else None,
            "converted_to_trip": (converted := _flag_to_optional_bool(dict(r).get("converted_to_trip"))),
            "outcome_status": _outcome_status(converted),
            "minutes_to_trip": float(dict(r)["minutes_to_trip"]) if dict(r).get("minutes_to_trip") is not None else None,
            "distance_to_recommendation_miles": float(dict(r)["distance_to_recommendation_miles"]) if dict(r).get("distance_to_recommendation_miles") is not None else None,
        }
        for r in rows
    ]


def get_admin_micro_recommendation_outcomes(
    limit: int = 200,
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    user_id: Optional[int] = None,
    outcome_status: Optional[str] = None,
    since_seconds: Optional[int] = None,
) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    where_clauses, params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=cluster_id,
        user_id=user_id,
        outcome_status=outcome_status,
        since_seconds=since_seconds,
        time_column="recommended_at",
        cluster_column="micro_cluster_id",
    )
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = _db_query_all(
        f"""
        SELECT
            id, user_id, recommended_at, zone_id, parent_hotspot_id, micro_cluster_id,
            micro_center_lat, micro_center_lng, score, confidence,
            converted_to_trip, minutes_to_trip, distance_to_recommendation_miles
        FROM micro_recommendation_outcomes
        {where_sql}
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        tuple(params + [clamped_limit]),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "user_id": int(dict(r)["user_id"]) if dict(r).get("user_id") is not None else None,
            "recommended_at": dict(r).get("recommended_at"),
            "recommended_at_iso": _to_iso(dict(r).get("recommended_at")),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "parent_hotspot_id": dict(r).get("parent_hotspot_id"),
            "micro_cluster_id": dict(r).get("micro_cluster_id"),
            "micro_center_lat": float(dict(r)["micro_center_lat"]) if dict(r).get("micro_center_lat") is not None else None,
            "micro_center_lng": float(dict(r)["micro_center_lng"]) if dict(r).get("micro_center_lng") is not None else None,
            "score": float(dict(r)["score"]) if dict(r).get("score") is not None else None,
            "confidence": float(dict(r)["confidence"]) if dict(r).get("confidence") is not None else None,
            "converted_to_trip": (converted := _flag_to_optional_bool(dict(r).get("converted_to_trip"))),
            "outcome_status": _outcome_status(converted),
            "minutes_to_trip": float(dict(r)["minutes_to_trip"]) if dict(r).get("minutes_to_trip") is not None else None,
            "distance_to_recommendation_miles": float(dict(r)["distance_to_recommendation_miles"]) if dict(r).get("distance_to_recommendation_miles") is not None else None,
        }
        for r in rows
    ]


def get_admin_system() -> Dict[str, Any]:
    frames = _frames_info()
    return {
        "backend_status": "ok",
        "timeline_ready": frames["timeline_ready"],
        "frame_count": frames["frame_count"],
        "frames_dir": frames["frames_dir"],
        "data_dir": frames["data_dir"],
        "leaderboard_status": _leaderboard_status(),
        "table_counts": {
            "users": _safe_count("users"),
            "presence": _safe_count("presence"),
            "events": _safe_count("events"),
            "pickup_logs": _safe_count("pickup_logs"),
            "chat_messages": _safe_count("chat_messages"),
            "hotspot_experiment_bins": _safe_count("hotspot_experiment_bins"),
            "micro_hotspot_experiment_bins": _safe_count("micro_hotspot_experiment_bins"),
            "recommendation_outcomes": _safe_count("recommendation_outcomes"),
            "micro_recommendation_outcomes": _safe_count("micro_recommendation_outcomes"),
        },
    }


def get_admin_experiment_summary(
    zone_id: Optional[int] = None,
    cluster_id: Optional[str] = None,
    user_id: Optional[int] = None,
    since_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    normalized_cluster_id = _normalize_cluster_id(cluster_id)
    clamped_since_seconds = _clamp_since_seconds(since_seconds)

    hotspot_base_clauses, hotspot_base_params = _build_bin_filter_clause_and_params(zone_id=zone_id)
    hotspot_recent_clauses, hotspot_recent_params = _build_bin_filter_clause_and_params(
        zone_id=zone_id,
        since_seconds=clamped_since_seconds,
    )

    hotspot_base_where = f"WHERE {' AND '.join(hotspot_base_clauses)}" if hotspot_base_clauses else ""
    hotspot_recent_where = f"WHERE {' AND '.join(hotspot_recent_clauses)}" if hotspot_recent_clauses else ""

    hotspot_recommended_expr = (
        "recommended = TRUE" if DB_BACKEND == "postgres" else "CAST(recommended AS INTEGER) = 1"
    )
    hotspot_row = _db_query_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM hotspot_experiment_bins {hotspot_base_where}) AS total_rows,
            COUNT(*) AS recent_rows,
            SUM(CASE WHEN {hotspot_recommended_expr} THEN 1 ELSE 0 END) AS recommended_rows,
            COUNT(DISTINCT zone_id) AS distinct_zone_count,
            AVG(final_score) AS avg_final_score,
            AVG(confidence) AS avg_confidence
        FROM hotspot_experiment_bins
        {hotspot_recent_where}
        """,
        tuple(hotspot_base_params + hotspot_recent_params),
    ) or {}

    micro_base_clauses, micro_base_params = _build_bin_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
    )
    micro_recent_clauses, micro_recent_params = _build_bin_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
        since_seconds=clamped_since_seconds,
    )
    micro_base_where = f"WHERE {' AND '.join(micro_base_clauses)}" if micro_base_clauses else ""
    micro_recent_where = f"WHERE {' AND '.join(micro_recent_clauses)}" if micro_recent_clauses else ""

    micro_recommended_expr = (
        "recommended = TRUE" if DB_BACKEND == "postgres" else "CAST(recommended AS INTEGER) = 1"
    )
    micro_row = _db_query_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM micro_hotspot_experiment_bins {micro_base_where}) AS total_rows,
            COUNT(*) AS recent_rows,
            SUM(CASE WHEN {micro_recommended_expr} THEN 1 ELSE 0 END) AS recommended_rows,
            COUNT(DISTINCT zone_id) AS distinct_zone_count,
            COUNT(DISTINCT cluster_id) AS distinct_cluster_count,
            AVG(final_score) AS avg_final_score,
            AVG(confidence) AS avg_confidence,
            AVG(intensity) AS avg_intensity
        FROM micro_hotspot_experiment_bins
        {micro_recent_where}
        """,
        tuple(micro_base_params + micro_recent_params),
    ) or {}

    outcome_base_clauses, outcome_base_params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
        user_id=user_id,
        time_column="recommended_at",
        cluster_column="cluster_id",
    )
    outcome_recent_clauses, outcome_recent_params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
        user_id=user_id,
        since_seconds=clamped_since_seconds,
        time_column="recommended_at",
        cluster_column="cluster_id",
    )
    outcome_base_where = f"WHERE {' AND '.join(outcome_base_clauses)}" if outcome_base_clauses else ""
    outcome_recent_where = f"WHERE {' AND '.join(outcome_recent_clauses)}" if outcome_recent_clauses else ""

    converted_expr = "converted_to_trip = TRUE" if DB_BACKEND == "postgres" else "CAST(converted_to_trip AS INTEGER) = 1"
    not_converted_expr = "converted_to_trip = FALSE" if DB_BACKEND == "postgres" else "CAST(converted_to_trip AS INTEGER) = 0"
    outcome_row = _db_query_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM recommendation_outcomes {outcome_base_where}) AS total_rows,
            SUM(CASE WHEN converted_to_trip IS NULL THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) AS converted_rows,
            SUM(CASE WHEN {not_converted_expr} THEN 1 ELSE 0 END) AS not_converted_rows,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
            AVG(CASE WHEN {converted_expr} THEN minutes_to_trip END) AS avg_minutes_to_trip_converted,
            AVG(CASE WHEN {converted_expr} THEN distance_to_recommendation_miles END) AS avg_distance_to_recommendation_miles_converted,
            COUNT(DISTINCT zone_id) AS distinct_zone_count,
            COUNT(DISTINCT cluster_id) AS distinct_cluster_count,
            COUNT(DISTINCT user_id) AS distinct_user_count
        FROM recommendation_outcomes
        {outcome_recent_where}
        """,
        tuple(outcome_base_params + outcome_recent_params),
    ) or {}

    micro_outcome_base_clauses, micro_outcome_base_params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
        user_id=user_id,
        time_column="recommended_at",
        cluster_column="micro_cluster_id",
    )
    micro_outcome_recent_clauses, micro_outcome_recent_params = _build_recent_filter_clause_and_params(
        zone_id=zone_id,
        cluster_id=normalized_cluster_id,
        user_id=user_id,
        since_seconds=clamped_since_seconds,
        time_column="recommended_at",
        cluster_column="micro_cluster_id",
    )
    micro_outcome_base_where = (
        f"WHERE {' AND '.join(micro_outcome_base_clauses)}" if micro_outcome_base_clauses else ""
    )
    micro_outcome_recent_where = (
        f"WHERE {' AND '.join(micro_outcome_recent_clauses)}" if micro_outcome_recent_clauses else ""
    )

    micro_outcome_row = _db_query_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM micro_recommendation_outcomes {micro_outcome_base_where}) AS total_rows,
            SUM(CASE WHEN converted_to_trip IS NULL THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) AS converted_rows,
            SUM(CASE WHEN {not_converted_expr} THEN 1 ELSE 0 END) AS not_converted_rows,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
            AVG(CASE WHEN {converted_expr} THEN minutes_to_trip END) AS avg_minutes_to_trip_converted,
            AVG(CASE WHEN {converted_expr} THEN distance_to_recommendation_miles END) AS avg_distance_to_recommendation_miles_converted,
            COUNT(DISTINCT zone_id) AS distinct_zone_count,
            COUNT(DISTINCT micro_cluster_id) AS distinct_micro_cluster_count,
            COUNT(DISTINCT parent_hotspot_id) AS distinct_parent_hotspot_count,
            COUNT(DISTINCT user_id) AS distinct_user_count
        FROM micro_recommendation_outcomes
        {micro_outcome_recent_where}
        """,
        tuple(micro_outcome_base_params + micro_outcome_recent_params),
    ) or {}

    def _as_int(value: Any) -> int:
        return int(value or 0)

    def _as_float(value: Any) -> Optional[float]:
        return float(value) if value is not None else None

    recommendation_resolved_rows = _as_int(outcome_row.get("resolved_rows"))
    recommendation_converted_rows = _as_int(outcome_row.get("converted_rows"))
    recommendation_conversion_rate = (
        recommendation_converted_rows / recommendation_resolved_rows if recommendation_resolved_rows > 0 else None
    )

    micro_resolved_rows = _as_int(micro_outcome_row.get("resolved_rows"))
    micro_converted_rows = _as_int(micro_outcome_row.get("converted_rows"))
    micro_conversion_rate = micro_converted_rows / micro_resolved_rows if micro_resolved_rows > 0 else None

    return {
        "filters": {
            "zone_id": int(zone_id) if zone_id is not None else None,
            "cluster_id": normalized_cluster_id,
            "user_id": int(user_id) if user_id is not None else None,
            "since_seconds": clamped_since_seconds,
        },
        "hotspot_experiment_bins": {
            "total_rows": _as_int(hotspot_row.get("total_rows")),
            "recent_rows": _as_int(hotspot_row.get("recent_rows")),
            "recommended_rows": _as_int(hotspot_row.get("recommended_rows")),
            "distinct_zone_count": _as_int(hotspot_row.get("distinct_zone_count")),
            "avg_final_score": _as_float(hotspot_row.get("avg_final_score")),
            "avg_confidence": _as_float(hotspot_row.get("avg_confidence")),
        },
        "micro_hotspot_experiment_bins": {
            "total_rows": _as_int(micro_row.get("total_rows")),
            "recent_rows": _as_int(micro_row.get("recent_rows")),
            "recommended_rows": _as_int(micro_row.get("recommended_rows")),
            "distinct_zone_count": _as_int(micro_row.get("distinct_zone_count")),
            "distinct_cluster_count": _as_int(micro_row.get("distinct_cluster_count")),
            "avg_final_score": _as_float(micro_row.get("avg_final_score")),
            "avg_confidence": _as_float(micro_row.get("avg_confidence")),
            "avg_intensity": _as_float(micro_row.get("avg_intensity")),
        },
        "recommendation_outcomes": {
            "total_rows": _as_int(outcome_row.get("total_rows")),
            "pending_rows": _as_int(outcome_row.get("pending_rows")),
            "converted_rows": recommendation_converted_rows,
            "not_converted_rows": _as_int(outcome_row.get("not_converted_rows")),
            "resolved_rows": recommendation_resolved_rows,
            "resolved_conversion_rate": recommendation_conversion_rate,
            "avg_minutes_to_trip_converted": _as_float(outcome_row.get("avg_minutes_to_trip_converted")),
            "avg_distance_to_recommendation_miles_converted": _as_float(
                outcome_row.get("avg_distance_to_recommendation_miles_converted")
            ),
            "distinct_zone_count": _as_int(outcome_row.get("distinct_zone_count")),
            "distinct_cluster_count": _as_int(outcome_row.get("distinct_cluster_count")),
            "distinct_user_count": _as_int(outcome_row.get("distinct_user_count")),
        },
        "micro_recommendation_outcomes": {
            "total_rows": _as_int(micro_outcome_row.get("total_rows")),
            "pending_rows": _as_int(micro_outcome_row.get("pending_rows")),
            "converted_rows": micro_converted_rows,
            "not_converted_rows": _as_int(micro_outcome_row.get("not_converted_rows")),
            "resolved_rows": micro_resolved_rows,
            "resolved_conversion_rate": micro_conversion_rate,
            "avg_minutes_to_trip_converted": _as_float(micro_outcome_row.get("avg_minutes_to_trip_converted")),
            "avg_distance_to_recommendation_miles_converted": _as_float(
                micro_outcome_row.get("avg_distance_to_recommendation_miles_converted")
            ),
            "distinct_zone_count": _as_int(micro_outcome_row.get("distinct_zone_count")),
            "distinct_micro_cluster_count": _as_int(micro_outcome_row.get("distinct_micro_cluster_count")),
            "distinct_parent_hotspot_count": _as_int(micro_outcome_row.get("distinct_parent_hotspot_count")),
            "distinct_user_count": _as_int(micro_outcome_row.get("distinct_user_count")),
        },
    }


def get_admin_experiment_rankings(
    since_seconds: Optional[int] = None,
    user_id: Optional[int] = None,
    zone_id: Optional[int] = None,
    limit: int = 20,
    min_resolved_rows: int = 3,
) -> Dict[str, Any]:
    clamped_since_seconds = _clamp_since_seconds(since_seconds)
    clamped_limit = max(1, min(100, int(limit)))
    clamped_min_resolved_rows = max(1, min(100, int(min_resolved_rows)))

    filters = {
        "since_seconds": clamped_since_seconds,
        "user_id": int(user_id) if user_id is not None else None,
        "zone_id": int(zone_id) if zone_id is not None else None,
        "limit": clamped_limit,
        "min_resolved_rows": clamped_min_resolved_rows,
    }

    where_clauses: list[str] = []
    params: list[Any] = []
    if user_id is not None:
        where_clauses.append("user_id = ?")
        params.append(int(user_id))
    if zone_id is not None:
        where_clauses.append("zone_id = ?")
        params.append(int(zone_id))
    if clamped_since_seconds is not None:
        cutoff = int(time.time()) - clamped_since_seconds
        if DB_BACKEND == "postgres":
            where_clauses.append("recommended_at >= to_timestamp(?)")
        else:
            where_clauses.append("recommended_at >= ?")
        params.append(cutoff)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    converted_expr = "converted_to_trip = TRUE" if DB_BACKEND == "postgres" else "CAST(converted_to_trip AS INTEGER) = 1"
    not_converted_expr = "converted_to_trip = FALSE" if DB_BACKEND == "postgres" else "CAST(converted_to_trip AS INTEGER) = 0"

    zone_rows = _db_query_all(
        f"""
        SELECT
            zone_id,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN converted_to_trip IS NULL THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) AS converted_rows,
            SUM(CASE WHEN {not_converted_expr} THEN 1 ELSE 0 END) AS not_converted_rows,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
            AVG(CASE WHEN {converted_expr} THEN minutes_to_trip END) AS avg_minutes_to_trip_converted,
            AVG(CASE WHEN {converted_expr} THEN distance_to_recommendation_miles END) AS avg_distance_to_recommendation_miles_converted,
            MAX(recommended_at) AS latest_recommended_at
        FROM recommendation_outcomes
        {where_sql}
        GROUP BY zone_id
        HAVING SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) >= ?
        ORDER BY
            (SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) * 1.0)
                / NULLIF(SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END), 0) DESC,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) DESC,
            MAX(recommended_at) DESC
        LIMIT ?
        """,
        tuple(params + [clamped_min_resolved_rows, clamped_limit]),
    )

    micro_rows = _db_query_all(
        f"""
        SELECT
            micro_cluster_id,
            zone_id,
            parent_hotspot_id,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN converted_to_trip IS NULL THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) AS converted_rows,
            SUM(CASE WHEN {not_converted_expr} THEN 1 ELSE 0 END) AS not_converted_rows,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
            AVG(CASE WHEN {converted_expr} THEN minutes_to_trip END) AS avg_minutes_to_trip_converted,
            AVG(CASE WHEN {converted_expr} THEN distance_to_recommendation_miles END) AS avg_distance_to_recommendation_miles_converted,
            MAX(recommended_at) AS latest_recommended_at
        FROM micro_recommendation_outcomes
        {where_sql}
        GROUP BY micro_cluster_id, zone_id, parent_hotspot_id
        HAVING SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) >= ?
        ORDER BY
            (SUM(CASE WHEN {converted_expr} THEN 1 ELSE 0 END) * 1.0)
                / NULLIF(SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END), 0) DESC,
            SUM(CASE WHEN converted_to_trip IS NOT NULL THEN 1 ELSE 0 END) DESC,
            MAX(recommended_at) DESC
        LIMIT ?
        """,
        tuple(params + [clamped_min_resolved_rows, clamped_limit]),
    )

    def _as_int(value: Any) -> int:
        return int(value or 0)

    def _as_float(value: Any) -> Optional[float]:
        return float(value) if value is not None else None

    hotspot_zone_rankings: List[Dict[str, Any]] = []
    for row_raw in zone_rows:
        row = dict(row_raw)
        resolved_rows = _as_int(row.get("resolved_rows"))
        converted_rows = _as_int(row.get("converted_rows"))
        hotspot_zone_rankings.append(
            {
                "zone_id": int(row["zone_id"]) if row.get("zone_id") is not None else None,
                "total_rows": _as_int(row.get("total_rows")),
                "pending_rows": _as_int(row.get("pending_rows")),
                "converted_rows": converted_rows,
                "not_converted_rows": _as_int(row.get("not_converted_rows")),
                "resolved_rows": resolved_rows,
                "resolved_conversion_rate": (converted_rows / resolved_rows) if resolved_rows > 0 else None,
                "avg_minutes_to_trip_converted": _as_float(row.get("avg_minutes_to_trip_converted")),
                "avg_distance_to_recommendation_miles_converted": _as_float(
                    row.get("avg_distance_to_recommendation_miles_converted")
                ),
                "latest_recommended_at": row.get("latest_recommended_at"),
                "latest_recommended_at_iso": _to_iso(row.get("latest_recommended_at")),
            }
        )

    micro_cluster_rankings: List[Dict[str, Any]] = []
    for row_raw in micro_rows:
        row = dict(row_raw)
        resolved_rows = _as_int(row.get("resolved_rows"))
        converted_rows = _as_int(row.get("converted_rows"))
        micro_cluster_rankings.append(
            {
                "micro_cluster_id": row.get("micro_cluster_id"),
                "zone_id": int(row["zone_id"]) if row.get("zone_id") is not None else None,
                "parent_hotspot_id": row.get("parent_hotspot_id"),
                "total_rows": _as_int(row.get("total_rows")),
                "pending_rows": _as_int(row.get("pending_rows")),
                "converted_rows": converted_rows,
                "not_converted_rows": _as_int(row.get("not_converted_rows")),
                "resolved_rows": resolved_rows,
                "resolved_conversion_rate": (converted_rows / resolved_rows) if resolved_rows > 0 else None,
                "avg_minutes_to_trip_converted": _as_float(row.get("avg_minutes_to_trip_converted")),
                "avg_distance_to_recommendation_miles_converted": _as_float(
                    row.get("avg_distance_to_recommendation_miles_converted")
                ),
                "latest_recommended_at": row.get("latest_recommended_at"),
                "latest_recommended_at_iso": _to_iso(row.get("latest_recommended_at")),
            }
        )

    return {
        "filters": filters,
        "hotspot_zone_rankings": hotspot_zone_rankings,
        "micro_cluster_rankings": micro_cluster_rankings,
    }
