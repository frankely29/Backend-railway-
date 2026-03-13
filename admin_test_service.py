from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from admin_service import get_admin_pickup_logs, get_admin_police_reports, get_admin_summary
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary
from core import DB_BACKEND, _db_query_all, _db_query_one


def _checked_at() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _response(ok: bool, test_name: str, summary: str, details: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": ok,
        "test_name": test_name,
        "checked_at": _checked_at(),
        "summary": summary,
        "details": details,
    }


def test_backend_status() -> Dict[str, Any]:
    try:
        row = _db_query_one("SELECT 1 AS ok")
        db_ready = bool(row and int(dict(row).get("ok", 0)) == 1)
    except Exception as exc:
        return _response(False, "backend-status", "Database probe failed", {"error": str(exc), "backend_status": "degraded"})

    return _response(
        True,
        "backend-status",
        "Backend is responding and database probe succeeded",
        {"backend_status": "ok", "db_backend": DB_BACKEND, "database_ready": db_ready},
    )


def test_timeline() -> Dict[str, Any]:
    frames_dir = Path(os.environ.get("FRAMES_DIR", str(Path(os.environ.get("DATA_DIR", "/data")) / "frames")))
    timeline_path = frames_dir / "timeline.json"
    timeline_ready = timeline_path.exists() and timeline_path.stat().st_size > 0 if timeline_path.exists() else False
    count = 0
    if timeline_ready:
        try:
            payload = json.loads(timeline_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                count = len(payload)
            elif isinstance(payload, dict):
                count = len(payload.get("frames", [])) if isinstance(payload.get("frames"), list) else len(payload)
        except Exception:
            count = 0

    return _response(
        timeline_ready,
        "timeline",
        "Timeline file is ready" if timeline_ready else "Timeline file is missing or empty",
        {"timeline_ready": timeline_ready, "timeline_count": count, "timeline_path": str(timeline_path)},
    )


def test_frame_current() -> Dict[str, Any]:
    frames_dir = Path(os.environ.get("FRAMES_DIR", str(Path(os.environ.get("DATA_DIR", "/data")) / "frames")))
    frame_files = sorted(frames_dir.glob("*.geojson")) if frames_dir.exists() else []
    frame_count = len(frame_files)
    return _response(
        frame_count > 0,
        "frame-current",
        "Frame data exists" if frame_count > 0 else "No frame files found",
        {
            "frame_available": frame_count > 0,
            "frame_count": frame_count,
            "latest_frame_file": frame_files[-1].name if frame_files else None,
        },
    )


def test_admin_auth(admin_user: Any) -> Dict[str, Any]:
    user = dict(admin_user)
    is_admin = bool(user.get("is_admin") in (1, True, "1", "true", "t"))
    return _response(
        is_admin,
        "admin-auth",
        "Admin identity resolved" if is_admin else "Identity is not admin",
        {
            "id": int(user["id"]) if user.get("id") is not None else None,
            "email": user.get("email"),
            "display_name": user.get("display_name"),
            "is_admin": is_admin,
        },
    )


def test_presence_summary() -> Dict[str, Any]:
    try:
        summary = get_admin_summary()
        online = int(summary.get("online_users", 0))
        ghosted = int(summary.get("ghosted_online_users", 0))
        return _response(True, "presence-summary", "Presence summary available", {"online_users": online, "ghosted_online_users": ghosted})
    except Exception as exc:
        return _response(False, "presence-summary", "Presence summary unavailable", {"error": str(exc)})


def test_presence_live() -> Dict[str, Any]:
    try:
        rows = _db_query_all("SELECT user_id FROM presence ORDER BY updated_at DESC LIMIT ?", (25,))
        ids = [int(dict(r)["user_id"]) for r in rows if dict(r).get("user_id") is not None]
        return _response(True, "presence-live", "Live presence rows readable", {"count": len(ids), "sample_user_ids": ids[:5]})
    except Exception as exc:
        return _response(False, "presence-live", "Live presence query failed", {"error": str(exc)})


def test_me(admin_user: Any) -> Dict[str, Any]:
    user = dict(admin_user)
    return _response(
        True,
        "me",
        "Current identity resolved",
        {
            "id": int(user["id"]) if user.get("id") is not None else None,
            "email": user.get("email"),
            "display_name": user.get("display_name"),
            "is_admin": bool(user.get("is_admin") in (1, True, "1", "true", "t")),
            "ghost_mode": bool(user.get("ghost_mode") in (1, True, "1", "true", "t")),
        },
    )


def test_trips_summary() -> Dict[str, Any]:
    try:
        summary = get_admin_trips_summary()
        total = int(summary.get("total_recorded_trips", 0))
        return _response(True, "trips-summary", "Trip summary query succeeded", {"total_recorded_trips": total, **summary})
    except Exception as exc:
        return _response(False, "trips-summary", "Trip summary query failed", {"error": str(exc)})


def test_trips_recent() -> Dict[str, Any]:
    try:
        items = get_admin_recent_trips(limit=20)
        return _response(True, "trips-recent", "Recent trips query succeeded", {"count": len(items), "sample": items[:3]})
    except Exception as exc:
        return _response(False, "trips-recent", "Recent trips query failed", {"error": str(exc)})


def test_police_reports() -> Dict[str, Any]:
    try:
        items = get_admin_police_reports(limit=20)
        return _response(True, "police-reports", "Police reports query succeeded", {"count": len(items)})
    except Exception as exc:
        return _response(False, "police-reports", "Police reports query failed", {"error": str(exc)})


def test_pickup_reports() -> Dict[str, Any]:
    try:
        items = get_admin_pickup_logs(limit=20)
        return _response(True, "pickup-reports", "Pickup reports query succeeded", {"count": len(items)})
    except Exception as exc:
        return _response(False, "pickup-reports", "Pickup reports query failed", {"error": str(exc)})
