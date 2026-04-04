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


def _to_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return str(ts)


def _safe_count(table: str) -> Optional[int]:
    try:
        row = _db_query_one(f"SELECT COUNT(*) AS c FROM {table}")
        return int(row["c"]) if row else 0
    except Exception:
        return None


def _recent_count(table: str, window_seconds: int = 86400) -> int:
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


def get_admin_hotspot_experiment_bins(limit: int = 200) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    rows = _db_query_all(
        """
        SELECT
            id, bin_time, zone_id, final_score, confidence,
            historical_component, live_component, same_timeslot_component,
            long_run_historical_component, recent_shape_component,
            outcome_modifier, quality_modifier, saturation_modifier,
            hotspot_limit_used, density_penalty, weighted_trip_count,
            unique_driver_count, recommended
        FROM hotspot_experiment_bins
        ORDER BY bin_time DESC, id DESC
        LIMIT ?
        """,
        (clamped_limit,),
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


def get_admin_micro_hotspot_experiment_bins(limit: int = 200) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    rows = _db_query_all(
        """
        SELECT
            id, bin_time, zone_id, cluster_id, final_score, confidence,
            weighted_trip_count, unique_driver_count, crowding_penalty,
            center_lat, center_lng, radius_m, intensity,
            baseline_component, live_component, same_timeslot_component,
            eta_alignment, recommended
        FROM micro_hotspot_experiment_bins
        ORDER BY bin_time DESC, id DESC
        LIMIT ?
        """,
        (clamped_limit,),
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


def get_admin_recommendation_outcomes(limit: int = 200) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    rows = _db_query_all(
        """
        SELECT
            id, user_id, recommended_at, zone_id, cluster_id,
            hotspot_center_lat, hotspot_center_lng, score, confidence,
            converted_to_trip, minutes_to_trip, distance_to_recommendation_miles
        FROM recommendation_outcomes
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        (clamped_limit,),
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
            "converted_to_trip": _flag_to_bool(dict(r).get("converted_to_trip")),
            "minutes_to_trip": float(dict(r)["minutes_to_trip"]) if dict(r).get("minutes_to_trip") is not None else None,
            "distance_to_recommendation_miles": float(dict(r)["distance_to_recommendation_miles"]) if dict(r).get("distance_to_recommendation_miles") is not None else None,
        }
        for r in rows
    ]


def get_admin_micro_recommendation_outcomes(limit: int = 200) -> List[Dict[str, Any]]:
    clamped_limit = max(1, min(1000, int(limit)))
    rows = _db_query_all(
        """
        SELECT
            id, user_id, recommended_at, zone_id, parent_hotspot_id, micro_cluster_id,
            micro_center_lat, micro_center_lng, score, confidence,
            converted_to_trip, minutes_to_trip, distance_to_recommendation_miles
        FROM micro_recommendation_outcomes
        ORDER BY recommended_at DESC, id DESC
        LIMIT ?
        """,
        (clamped_limit,),
    )
    return [
        {
            "id": int(dict(r)["id"]) if dict(r).get("id") is not None else None,
            "user_id": int(dict(r)["user_id"]) if dict(r).get("user_id") is not None else None,
            "recommended_at": dict(r).get("recommended_at"),
            "recommended_at_iso": _to_iso(dict(r).get("recommended_at")),
            "zone_id": int(dict(r)["zone_id"]) if dict(r).get("zone_id") is not None else None,
            "parent_hotspot_id": int(dict(r)["parent_hotspot_id"]) if dict(r).get("parent_hotspot_id") is not None else None,
            "micro_cluster_id": dict(r).get("micro_cluster_id"),
            "micro_center_lat": float(dict(r)["micro_center_lat"]) if dict(r).get("micro_center_lat") is not None else None,
            "micro_center_lng": float(dict(r)["micro_center_lng"]) if dict(r).get("micro_center_lng") is not None else None,
            "score": float(dict(r)["score"]) if dict(r).get("score") is not None else None,
            "confidence": float(dict(r)["confidence"]) if dict(r).get("confidence") is not None else None,
            "converted_to_trip": _flag_to_bool(dict(r).get("converted_to_trip")),
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
