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
        row = _db_query_one("SELECT COUNT(*) AS c FROM users WHERE is_admin = ?", (True if DB_BACKEND == "postgres" else 1,))
        admin_users = int(row["c"]) if row else 0
    except Exception:
        admin_users = 0

    try:
        if DB_BACKEND == "postgres":
            counts = _db_query_one(
                """
                SELECT COUNT(*) AS online_count,
                       SUM(CASE WHEN COALESCE(u.ghost_mode, FALSE) THEN 1 ELSE 0 END) AS ghosted_count
                FROM presence p
                LEFT JOIN users u ON u.id = p.user_id
                WHERE EXTRACT(EPOCH FROM (NOW() - p.updated_at)) <= ?
                """,
                (300,),
            )
        else:
            counts = _db_query_one(
                """
                SELECT COUNT(*) AS online_count,
                       SUM(CASE WHEN COALESCE(u.ghost_mode, 0) = 1 THEN 1 ELSE 0 END) AS ghosted_count
                FROM presence p
                LEFT JOIN users u ON u.id = p.user_id
                WHERE p.updated_at >= ?
                """,
                (int(time.time()) - 300,),
            )
        online_users = int(counts["online_count"] or 0) if counts else 0
        ghosted_online_users = int(counts["ghosted_count"] or 0) if counts else 0
    except Exception:
        online_users = 0
        ghosted_online_users = 0

    frames = _frames_info()
    return {
        "total_users": total_users,
        "admin_users": admin_users,
        "online_users": online_users,
        "ghosted_online_users": ghosted_online_users,
        "police_reports_recent_count": _recent_count("events"),
        "pickup_logs_recent_count": _recent_count("pickup_logs"),
        "timeline_ready": frames["timeline_ready"],
        "frame_count": frames["frame_count"],
        "leaderboard_status": _leaderboard_status(),
        "backend_status": "ok",
    }


def get_admin_users(limit: int = 500) -> List[Dict[str, Any]]:
    rows = _db_query_all(
        """
        SELECT id, email, display_name, is_admin, is_suspended, ghost_mode, avatar_url, created_at
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
        },
    }
