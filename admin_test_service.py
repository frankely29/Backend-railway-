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
    timeline_path = frames_dir / "timeline.json"

    file_frames = sorted(frames_dir.glob("frame_*.json")) if frames_dir.exists() else []
    file_frame_count = len(file_frames)
    latest_frame_file = file_frames[-1].name if file_frames else None

    timeline_ready = timeline_path.exists() and timeline_path.stat().st_size > 0 if timeline_path.exists() else False
    frame_api_ok = False
    frame_features_count = 0
    frame_available = False

    if timeline_ready:
        try:
            timeline_payload = json.loads(timeline_path.read_text(encoding="utf-8"))

            timeline_items = []
            if isinstance(timeline_payload, dict) and isinstance(timeline_payload.get("timeline"), list):
                timeline_items = timeline_payload.get("timeline") or []
            elif isinstance(timeline_payload, list):
                timeline_items = timeline_payload

            if timeline_items:
                candidate_indices = [0, len(timeline_items) - 1]
                seen_indices = set()
                for idx in candidate_indices:
                    if idx in seen_indices:
                        continue
                    seen_indices.add(idx)

                    frame_path = frames_dir / f"frame_{idx:06d}.json"
                    if not frame_path.exists() or frame_path.stat().st_size == 0:
                        continue

                    frame_payload = json.loads(frame_path.read_text(encoding="utf-8"))
                    features = []
                    if isinstance(frame_payload, dict):
                        polygons = frame_payload.get("polygons")
                        if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
                            features = polygons.get("features") or []
                        elif isinstance(frame_payload.get("features"), list):
                            features = frame_payload.get("features") or []

                    if isinstance(frame_payload, dict):
                        frame_api_ok = True
                        frame_features_count = len(features)
                        frame_available = frame_features_count > 0
                        if frame_available:
                            break
        except Exception:
            frame_api_ok = False
            frame_features_count = 0
            frame_available = False

    ok = frame_api_ok and frame_available
    if ok:
        summary = "Current frame API returned usable data"
    else:
        summary = "Frame endpoint is unavailable or returned unusable data"

    return _response(
        ok,
        "frame-current",
        summary,
        {
            "frame_api_ok": frame_api_ok,
            "frame_features_count": frame_features_count,
            "frame_available": frame_available,
            "file_frame_count": file_frame_count,
            "latest_frame_file": latest_frame_file,
            "timeline_ready": timeline_ready,
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


def test_presence_endpoint() -> Dict[str, Any]:
    from main import _presence_visibility_snapshot

    snapshot = _presence_visibility_snapshot(max_age_sec=300)
    details = {
        "db_backend": snapshot.get("db_backend"),
        "visible_count": int(snapshot.get("visible_count") or 0),
        "online_count": int(snapshot.get("online_count") or 0),
        "ghosted_count": int(snapshot.get("ghosted_count") or 0),
        "sample_user_ids": snapshot.get("sample_user_ids") or [],
        "sample_display_names": snapshot.get("sample_display_names") or [],
        "sql_mode": snapshot.get("sql_mode"),
    }
    if snapshot.get("ok"):
        return _response(True, "presence-endpoint", "Presence visibility query succeeded", details)
    details["error"] = snapshot.get("error")
    return _response(False, "presence-endpoint", "Presence visibility query failed", details)


def test_pickup_overlay_endpoint(admin_user: Any) -> Dict[str, Any]:
    from main import _recent_pickups_payload

    try:
        payload = _recent_pickups_payload(limit=30, zone_sample_limit=100, debug=1, viewer=admin_user)
        zone_stats = payload.get("zone_stats") if isinstance(payload, dict) else []
        zone_hotspots = payload.get("zone_hotspots") if isinstance(payload, dict) else {}
        zone_features = zone_hotspots.get("features") if isinstance(zone_hotspots, dict) else []
        micro_hotspots = payload.get("micro_hotspots") if isinstance(payload, dict) else []
        sampled_zone_ids = [
            int(item.get("zone_id"))
            for item in (zone_stats if isinstance(zone_stats, list) else [])[:5]
            if isinstance(item, dict) and item.get("zone_id") is not None
        ]
        details = {
            "item_count": int(payload.get("count") or 0),
            "zone_stats_count": len(zone_stats) if isinstance(zone_stats, list) else 0,
            "zone_hotspot_count": len(zone_features) if isinstance(zone_features, list) else 0,
            "micro_hotspot_count": len(micro_hotspots) if isinstance(micro_hotspots, list) else 0,
            "sampled_zone_ids": sampled_zone_ids,
        }
        return _response(True, "pickup-overlay-endpoint", "Pickup overlay query succeeded", details)
    except Exception as exc:
        return _response(
            False,
            "pickup-overlay-endpoint",
            "Pickup overlay query failed",
            {
                "item_count": 0,
                "zone_stats_count": 0,
                "zone_hotspot_count": 0,
                "micro_hotspot_count": 0,
                "sampled_zone_ids": [],
                "error": str(exc),
            },
        )
