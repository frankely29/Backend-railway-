from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict

from admin_service import get_admin_pickup_logs, get_admin_police_reports, get_admin_summary
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary
from artifact_freshness import evaluate_artifact_freshness
from artifact_storage_service import get_artifact_storage_report
from core import DB_BACKEND, _db_query_all, _db_query_one
from artifact_db_store import generated_artifact_present, load_generated_artifact


def _checked_at() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_admin_response(ok: bool, test_name: str, summary: str, details: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": ok,
        "test_name": test_name,
        "checked_at": _checked_at(),
        "summary": summary,
        "details": details,
    }


def _response(ok: bool, test_name: str, summary: str, details: Dict[str, Any]) -> Dict[str, Any]:
    return build_admin_response(ok, test_name, summary, details)


def _frames_dir() -> Path:
    return Path(os.environ.get("FRAMES_DIR", str(Path(os.environ.get("DATA_DIR", "/data")) / "frames")))


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/data"))


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _active_month_context() -> Dict[str, Any]:
    """
    Fix J: resolve the currently-active month's authoritative paths.
    Uses the same resolver main.py's /status endpoint uses, so admin tests
    reflect the true monthly-partition state rather than legacy /data/frames.

    Returns a dict with:
        - month_key: str or None
        - timeline_path: Path (may not exist if month not ready)
        - frame_cache_dir: Path (may not exist if month not ready)
        - bootstrap_state: dict (from _month_bootstrap_state, empty if no month)

    Safe to call even during startup — catches all exceptions and returns
    a zero-state context so callers can still produce a meaningful failure
    response instead of crashing.
    """
    try:
        from main import (
            _available_source_month_keys,
            _month_bootstrap_state,
            _month_frame_cache_dir,
            _month_timeline_path,
            resolve_active_month_key,
            NYC_TZ,
        )
        from datetime import datetime, timezone as _tz_mod

        source_month_keys = _available_source_month_keys()
        active_month_key = resolve_active_month_key(
            datetime.now(_tz_mod.utc).astimezone(NYC_TZ),
            source_month_keys,
        )
        if not active_month_key:
            return {
                "month_key": None,
                "timeline_path": None,
                "frame_cache_dir": None,
                "bootstrap_state": {},
            }
        return {
            "month_key": str(active_month_key),
            "timeline_path": _month_timeline_path(active_month_key),
            "frame_cache_dir": _month_frame_cache_dir(active_month_key),
            "bootstrap_state": _month_bootstrap_state(active_month_key) or {},
        }
    except Exception as exc:
        return {
            "month_key": None,
            "timeline_path": None,
            "frame_cache_dir": None,
            "bootstrap_state": {},
            "error": str(exc),
        }


def _low_space_context(generate_error: str | None = None) -> Dict[str, Any]:
    storage_report = get_artifact_storage_report(_data_dir(), _frames_dir())
    error_text = str(generate_error or "")
    low_space = bool(storage_report.get("low_space")) or "no space left on device" in error_text.lower() or "errno 28" in error_text.lower()
    return {"low_space": low_space, "storage_report": storage_report}


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


def test_build_sync() -> Dict[str, Any]:
    from main import _artifact_freshness_snapshot, _backend_identity_snapshot

    frames_dir = _frames_dir()
    timeline_path = frames_dir / "timeline.json"
    manifest_path = frames_dir / "scoring_shadow_manifest.json"

    identity = _backend_identity_snapshot(_artifact_freshness_snapshot())
    backend_build_id = identity.get("backend_build_id")
    backend_release = identity.get("backend_release")
    timeline_present = timeline_path.exists() and timeline_path.stat().st_size > 0 if timeline_path.exists() else False
    manifest_present = manifest_path.exists() and manifest_path.stat().st_size > 0 if manifest_path.exists() else False
    identity_available = bool(backend_build_id and backend_release)

    return _response(
        identity_available,
        "build-sync",
        "Backend build identity available" if identity_available else "Backend build identity missing",
        {
            "backend_build_id": backend_build_id,
            "backend_release": backend_release,
            "backend_identity_source": identity.get("source"),
            "frames_dir": str(frames_dir),
            "manifest_present": manifest_present,
            "timeline_present": timeline_present,
        },
    )


def test_storage_health() -> Dict[str, Any]:
    report = get_artifact_storage_report(_data_dir(), _frames_dir())
    ok = not bool(report.get("low_space"))
    return _response(
        ok,
        "storage-health",
        "Storage has enough headroom for artifact rebuild." if ok else "Storage is too full for artifact rebuild.",
        {
            "storage_report": report,
            "cleanup_candidates": report.get("cleanup_candidates") or [],
        },
    )


def test_timeline() -> Dict[str, Any]:
    """
    Fix J: read timeline from the active month's partition path instead of
    the legacy /data/frames/timeline.json. The monthly-partition model is
    the authoritative source since the system moved to exact_store months.
    """
    ctx = _active_month_context()
    active_month_key = ctx.get("month_key")
    timeline_path = ctx.get("timeline_path")

    if not active_month_key or timeline_path is None:
        return _response(
            False,
            "timeline",
            "No active month resolved — parquet data may be missing.",
            {
                "timeline_ready": False,
                "timeline_count": 0,
                "active_month_key": None,
                "timeline_path": None,
                "resolve_error": ctx.get("error"),
            },
        )

    timeline_ready = bool(
        timeline_path.exists()
        and timeline_path.is_file()
        and timeline_path.stat().st_size > 0
    )
    count = 0
    if timeline_ready:
        try:
            payload = json.loads(timeline_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("timeline"), list):
                count = len(payload["timeline"])
            elif isinstance(payload, dict) and isinstance(payload.get("frames"), list):
                count = len(payload["frames"])
            elif isinstance(payload, list):
                count = len(payload)
        except Exception:
            count = 0

    return _response(
        timeline_ready,
        "timeline",
        (
            f"Active month timeline is ready ({count} entries)."
            if timeline_ready
            else "Active month timeline file is missing or empty."
        ),
        {
            "timeline_ready": timeline_ready,
            "timeline_count": count,
            "active_month_key": active_month_key,
            "timeline_path": str(timeline_path),
        },
    )

def test_frame_current() -> Dict[str, Any]:
    """
    Fix J: verify the active month's frame_cache directory has files present
    and that at least one frame can be loaded. Frame cache files in the
    monthly partition model are named frame_{idx:05d}_{safe_time}.json, not
    the legacy frame_{idx:06d}.json in /data/frames.
    """
    ctx = _active_month_context()
    active_month_key = ctx.get("month_key")
    timeline_path = ctx.get("timeline_path")
    frame_cache_dir = ctx.get("frame_cache_dir")

    if not active_month_key or timeline_path is None or frame_cache_dir is None:
        return _response(
            False,
            "frame-current",
            "No active month resolved — cannot probe current frame.",
            {
                "frame_api_ok": False,
                "frame_features_count": 0,
                "frame_available": False,
                "file_frame_count": 0,
                "latest_frame_file": None,
                "timeline_ready": False,
                "active_month_key": None,
            },
        )

    timeline_ready = bool(
        timeline_path.exists()
        and timeline_path.is_file()
        and timeline_path.stat().st_size > 0
    )

    file_frames = sorted(frame_cache_dir.glob("frame_*.json")) if frame_cache_dir.exists() else []
    file_frame_count = len(file_frames)
    latest_frame_file = file_frames[-1].name if file_frames else None

    frame_api_ok = False
    frame_features_count = 0
    frame_available = False

    if timeline_ready and file_frames:
        try:
            # Load the last available frame file and count features.
            frame_payload = json.loads(file_frames[-1].read_text(encoding="utf-8"))
            features: list = []
            if isinstance(frame_payload, dict):
                polygons = frame_payload.get("polygons")
                if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
                    features = polygons.get("features") or []
                elif isinstance(frame_payload.get("features"), list):
                    features = frame_payload.get("features") or []
                frame_api_ok = True
                frame_features_count = len(features)
                frame_available = frame_features_count > 0
        except Exception:
            frame_api_ok = False
            frame_features_count = 0
            frame_available = False

    ok = bool(frame_available)
    return _response(
        ok,
        "frame-current",
        (
            f"Active month frame is loadable ({frame_features_count} features in latest frame)."
            if ok
            else "Active month frame is unavailable or returned unusable data."
        ),
        {
            "frame_api_ok": frame_api_ok,
            "frame_features_count": frame_features_count,
            "frame_available": frame_available,
            "file_frame_count": file_frame_count,
            "latest_frame_file": latest_frame_file,
            "timeline_ready": timeline_ready,
            "active_month_key": active_month_key,
            "frame_cache_dir": str(frame_cache_dir),
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


def test_score_manifest() -> Dict[str, Any]:
    """
    Fix J: read scoring_shadow_manifest from the generated_artifact_store
    (Postgres, DB-stored) instead of the legacy filesystem path. After
    Fix D, the manifest is DB-authoritative; the filesystem file is no
    longer kept on the volume.
    """
    from main import _get_state

    expected_visible = [
        "citywide_v3",
        "manhattan_v3",
        "bronx_wash_heights_v3",
        "queens_v3",
        "brooklyn_v3",
        "staten_island_v3",
    ]

    manifest_present_in_db = bool(generated_artifact_present("scoring_shadow_manifest"))

    freshness = evaluate_artifact_freshness(
        repo_root=_repo_root(),
        data_dir=_data_dir(),
        frames_dir=_frames_dir(),
        bin_minutes=int(os.environ.get("DEFAULT_BIN_MINUTES", "20")),
        min_trips_per_window=int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25")),
    )

    low_space_ctx = _low_space_context((_get_state().get("error") or ""))
    if not manifest_present_in_db or not freshness.get("fresh"):
        likely_cause = "low_space_volume" if low_space_ctx["low_space"] else "generic_stale_artifacts"
        return _response(
            False,
            "score-manifest",
            (
                "Generated artifacts are stale or missing due to low volume space."
                if likely_cause == "low_space_volume"
                else "Generated artifacts are stale or missing."
            ),
            {
                "manifest_source": "generated_artifact_store",
                "manifest_present": manifest_present_in_db,
                "visible_profiles_live": [],
                "default_citywide_profile": None,
                "mismatches": freshness.get("reason_codes") or ["manifest missing in generated_artifact_store"],
                "likely_cause": likely_cause,
                "storage_report": low_space_ctx["storage_report"],
            },
        )

    try:
        artifact = load_generated_artifact("scoring_shadow_manifest")
        manifest = (artifact or {}).get("payload") or {}
        if not isinstance(manifest, dict):
            raise ValueError("scoring_shadow_manifest payload is not a dict")
    except Exception as exc:
        return _response(
            False,
            "score-manifest",
            "Scoring manifest does not match the expected v3 rollout.",
            {
                "manifest_source": "generated_artifact_store",
                "manifest_present": True,
                "visible_profiles_live": [],
                "default_citywide_profile": None,
                "mismatches": [f"manifest unreadable: {exc}"],
            },
        )

    visible_profiles_live = manifest.get("visible_profiles_live")
    default_citywide_profile = manifest.get("default_citywide_profile")

    mismatches: list[str] = []
    if default_citywide_profile != "citywide_v3":
        mismatches.append("default_citywide_profile != citywide_v3")
    if not isinstance(visible_profiles_live, list):
        mismatches.append("visible_profiles_live must be a list")
        visible_profiles_live = []

    missing_profiles = [profile for profile in expected_visible if profile not in visible_profiles_live]
    if missing_profiles:
        mismatches.append(f"missing visible_profiles_live entries: {missing_profiles}")

    ok = len(mismatches) == 0
    return _response(
        ok,
        "score-manifest",
        "Scoring manifest matches the expected v3 rollout." if ok else "Scoring manifest does not match the expected v3 rollout.",
        {
            "manifest_source": "generated_artifact_store",
            "manifest_present": True,
            "visible_profiles_live": visible_profiles_live,
            "default_citywide_profile": default_citywide_profile,
            "mismatches": mismatches,
        },
    )

def test_score_sql_definitions() -> Dict[str, Any]:
    from zone_earnings_engine import build_zone_earnings_shadow_sql
    from zone_mode_profiles import ZONE_MODE_PROFILES

    try:
        sql = build_zone_earnings_shadow_sql(
            ["/tmp/dummy.parquet"],
            bin_minutes=20,
            min_trips_per_window=25,
            profile=ZONE_MODE_PROFILES["citywide_v3"],
            citywide_v3_profile=ZONE_MODE_PROFILES["citywide_v3"],
            manhattan_profile=ZONE_MODE_PROFILES["manhattan_v3"],
            bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v3"],
            queens_profile=ZONE_MODE_PROFILES["queens_v3"],
            brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v3"],
            staten_island_profile=ZONE_MODE_PROFILES["staten_island_v3"],
            manhattan_v3_profile=ZONE_MODE_PROFILES["manhattan_v3"],
            bronx_wash_heights_v3_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v3"],
            queens_v3_profile=ZONE_MODE_PROFILES["queens_v3"],
            brooklyn_v3_profile=ZONE_MODE_PROFILES["brooklyn_v3"],
            staten_island_v3_profile=ZONE_MODE_PROFILES["staten_island_v3"],
            available_columns={"request_datetime", "shared_match_flag", "shared_request_flag"},
        )
    except Exception as exc:
        return _response(
            False,
            "score-sql-definitions",
            "Score SQL definitions do not match the intended trap-aware logic.",
            {
                "contains_trip_time_lte_720": False,
                "contains_trip_time_gte_1200": False,
                "contains_retention_rank_asc": False,
                "contains_retention_rank_desc": False,
                "error": str(exc),
            },
        )

    contains_retention_rank_asc = "same_zone_retention_penalty_rn" in sql and "ORDER BY same_zone_dropoff_share" in sql
    contains_retention_rank_desc = "ORDER BY same_zone_dropoff_share DESC" in sql and "same_zone_retention_penalty_rn" in sql

    details = {
        "contains_trip_time_lte_720": "trip_time <= 720.0" in sql,
        "contains_trip_time_gte_1200": "trip_time >= 1200" in sql,
        "contains_retention_rank_asc": contains_retention_rank_asc,
        "contains_retention_rank_desc": contains_retention_rank_desc,
    }
    ok = (
        details["contains_trip_time_lte_720"]
        and details["contains_trip_time_gte_1200"]
        and details["contains_retention_rank_asc"]
        and not details["contains_retention_rank_desc"]
    )

    return _response(
        ok,
        "score-sql-definitions",
        "Score SQL definitions match the intended trap-aware logic." if ok else "Score SQL definitions do not match the intended trap-aware logic.",
        details,
    )

def test_zone_geometry_metrics() -> Dict[str, Any]:
    from zone_geometry_metrics import build_zone_geometry_metrics_rows

    zones_geojson_path = _data_dir() / "taxi_zones.geojson"
    if not zones_geojson_path.exists() or zones_geojson_path.stat().st_size == 0:
        return _response(
            False,
            "zone-geometry-metrics",
            "Zone geometry metrics are missing or invalid.",
            {
                "zones_geojson_path": str(zones_geojson_path),
                "total_rows": 0,
                "positive_area_rows": 0,
                "null_or_zero_area_rows": 0,
                "min_area_sq_miles": None,
                "max_area_sq_miles": None,
            },
        )

    try:
        rows = build_zone_geometry_metrics_rows(zones_geojson_path)
    except Exception:
        rows = []

    areas = [float(row.get("zone_area_sq_miles")) for row in rows if row.get("zone_area_sq_miles") is not None]
    positive_areas = [area for area in areas if area > 0]
    min_area = min(positive_areas) if positive_areas else None
    max_area = max(positive_areas) if positive_areas else None

    ok = len(rows) > 200 and len(positive_areas) > 200 and min_area is not None and max_area is not None and max_area > min_area > 0

    return _response(
        ok,
        "zone-geometry-metrics",
        "Zone geometry metrics look valid." if ok else "Zone geometry metrics are missing or invalid.",
        {
            "zones_geojson_path": str(zones_geojson_path),
            "total_rows": len(rows),
            "positive_area_rows": len(positive_areas),
            "null_or_zero_area_rows": len(rows) - len(positive_areas),
            "min_area_sq_miles": min_area,
            "max_area_sq_miles": max_area,
        },
    )

def test_score_frame_integrity() -> Dict[str, Any]:
    """
    Fix J: sample frames from the active month's frame_cache directory
    instead of the legacy /data/frames. Frame cache filenames follow
    frame_{idx:05d}_{safe_time}.json; use glob to find them by index.
    """
    from main import _generate_lock_snapshot, _get_state
    from build_hotspot import bucket_and_color_from_rating

    _ = _generate_lock_snapshot  # kept for API compatibility
    _ = _get_state

    ctx = _active_month_context()
    active_month_key = ctx.get("month_key")
    timeline_path = ctx.get("timeline_path")
    frame_cache_dir = ctx.get("frame_cache_dir")

    if not active_month_key or timeline_path is None or frame_cache_dir is None:
        return _response(
            False,
            "score-frame-integrity",
            "Sampled frame features contain invalid or missing score fields.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": ["no active month resolved"],
                "active_month_key": None,
            },
        )

    if not timeline_path.exists() or timeline_path.stat().st_size == 0:
        return _response(
            False,
            "score-frame-integrity",
            "Sampled frame features contain invalid or missing score fields.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": [f"timeline missing: {timeline_path}"],
                "active_month_key": active_month_key,
            },
        )

    try:
        timeline_payload = json.loads(timeline_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _response(
            False,
            "score-frame-integrity",
            "Sampled frame features contain invalid or missing score fields.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": [f"timeline unreadable: {exc}"],
                "active_month_key": active_month_key,
            },
        )

    timeline_items: list[Any] = []
    if isinstance(timeline_payload, dict) and isinstance(timeline_payload.get("timeline"), list):
        timeline_items = timeline_payload.get("timeline") or []
    elif isinstance(timeline_payload, dict) and isinstance(timeline_payload.get("frames"), list):
        timeline_items = timeline_payload.get("frames") or []
    elif isinstance(timeline_payload, list):
        timeline_items = timeline_payload

    if not timeline_items:
        return _response(
            False,
            "score-frame-integrity",
            "Sampled frame features contain invalid or missing score fields.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": ["timeline contains no frames"],
                "active_month_key": active_month_key,
            },
        )

    sampled_frame_indices = sorted(set([0, len(timeline_items) // 2, len(timeline_items) - 1]))
    required_rating_fields = [
        "earnings_shadow_rating_citywide_v3",
        "earnings_shadow_rating_manhattan_v3",
        "earnings_shadow_rating_bronx_wash_heights_v3",
        "earnings_shadow_rating_queens_v3",
        "earnings_shadow_rating_brooklyn_v3",
        "earnings_shadow_rating_staten_island_v3",
    ]
    required_metric_fields = [
        "pickups_now_shadow",
        "next_pickups_shadow",
        "zone_area_sq_miles_shadow",
        "pickups_per_sq_mile_now_shadow",
        "pickups_per_sq_mile_next_shadow",
        "long_trip_share_20plus_shadow",
        "same_zone_dropoff_share_shadow",
        "demand_density_now_n_shadow",
        "demand_density_next_n_shadow",
        "same_zone_retention_penalty_n_shadow",
    ]
    confidence_fields = [
        "earnings_shadow_confidence_citywide_v3",
        "earnings_shadow_confidence_manhattan_v3",
        "earnings_shadow_confidence_bronx_wash_heights_v3",
        "earnings_shadow_confidence_queens_v3",
        "earnings_shadow_confidence_brooklyn_v3",
        "earnings_shadow_confidence_staten_island_v3",
    ]
    shadow_rating_families = [
        "citywide_v2",
        "citywide_v3",
        "manhattan_v2",
        "manhattan_v3",
        "bronx_wash_heights_v2",
        "bronx_wash_heights_v3",
        "queens_v2",
        "queens_v3",
        "brooklyn_v2",
        "brooklyn_v3",
        "staten_island_v2",
        "staten_island_v3",
    ]

    sampled_features: list[Dict[str, Any]] = []
    violations: list[str] = []

    for frame_idx in sampled_frame_indices:
        # Fix J: frame cache files use {idx:05d}_{safe_time}.json pattern.
        # Glob by index prefix to find the file regardless of the time suffix.
        frame_matches = sorted(frame_cache_dir.glob(f"frame_{frame_idx:05d}_*.json"))
        if not frame_matches:
            violations.append(f"missing frame_{frame_idx:05d}_*.json in {frame_cache_dir}")
            continue
        frame_path = frame_matches[0]
        if not frame_path.is_file() or frame_path.stat().st_size == 0:
            violations.append(f"empty frame file: {frame_path.name}")
            continue

        try:
            frame_payload = json.loads(frame_path.read_text(encoding="utf-8"))
        except Exception as exc:
            violations.append(f"frame_{frame_idx:05d} unreadable: {exc}")
            continue

        features: list = []
        if isinstance(frame_payload, dict):
            polygons = frame_payload.get("polygons")
            if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
                features = polygons.get("features") or []
            elif isinstance(frame_payload.get("features"), list):
                features = frame_payload.get("features") or []

        if not features:
            violations.append(f"frame_{frame_idx:05d} has no features")
            continue

        for feature in features[:5]:  # sample first 5 features per frame
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
            sampled_features.append(props)

            # Check required rating fields
            for field in required_rating_fields:
                if field not in props:
                    violations.append(f"frame_{frame_idx:05d} missing rating field: {field}")

            # Check required metric fields
            for field in required_metric_fields:
                if field not in props:
                    violations.append(f"frame_{frame_idx:05d} missing metric field: {field}")

            # Check confidence fields
            for field in confidence_fields:
                if field not in props:
                    violations.append(f"frame_{frame_idx:05d} missing confidence field: {field}")

            # Check rating/bucket/color consistency for each family
            for family in shadow_rating_families:
                rating_key = f"earnings_shadow_rating_{family}"
                bucket_key = f"earnings_shadow_bucket_{family}"
                color_key = f"earnings_shadow_color_{family}"
                if rating_key in props and bucket_key in props and color_key in props:
                    try:
                        rating_value = props.get(rating_key)
                        if rating_value is not None:
                            expected_bucket, expected_color = bucket_and_color_from_rating(float(rating_value))
                            if props.get(bucket_key) != expected_bucket:
                                violations.append(
                                    f"frame_{frame_idx:05d} {family} bucket mismatch: "
                                    f"got {props.get(bucket_key)}, expected {expected_bucket}"
                                )
                            if props.get(color_key) != expected_color:
                                violations.append(
                                    f"frame_{frame_idx:05d} {family} color mismatch: "
                                    f"got {props.get(color_key)}, expected {expected_color}"
                                )
                    except Exception:
                        pass

    violation_count = len(violations)
    ok = violation_count == 0 and len(sampled_features) > 0
    return _response(
        ok,
        "score-frame-integrity",
        (
            f"Sampled {len(sampled_features)} features across {len(sampled_frame_indices)} frames; all validations passed."
            if ok
            else "Sampled frame features contain invalid or missing score fields."
        ),
        {
            "sampled_frame_indices": sampled_frame_indices,
            "sampled_feature_count": len(sampled_features),
            "violation_count": violation_count,
            "first_violations": violations[:10],
            "active_month_key": active_month_key,
            "frame_cache_dir": str(frame_cache_dir),
        },
    )

def test_generated_artifact_sync() -> Dict[str, Any]:
    from main import _generate_lock_snapshot, _get_state

    frames_dir = _frames_dir()
    freshness = evaluate_artifact_freshness(
        repo_root=_repo_root(),
        data_dir=_data_dir(),
        frames_dir=frames_dir,
        bin_minutes=int(os.environ.get("DEFAULT_BIN_MINUTES", "20")),
        min_trips_per_window=int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25")),
    )
    ok = bool(freshness.get("fresh"))
    lock_snapshot = _generate_lock_snapshot()
    stale_lock = bool(lock_snapshot.get("lock_present")) and not bool(lock_snapshot.get("thread_alive"))
    low_space_ctx = _low_space_context((_get_state().get("error") or ""))
    likely_cause = None
    if not ok and low_space_ctx["low_space"]:
        likely_cause = "low_space_volume"
    elif not ok and stale_lock:
        likely_cause = "stale_generate_lock"
    elif not ok:
        likely_cause = "generic_stale_artifacts"
    summary = freshness.get("summary") or ("Generated frame artifacts match the deployed v3 code." if ok else "Generated frame artifacts are stale.")
    if not ok and likely_cause == "low_space_volume":
        summary = f"{summary} Likely cause: low space on the mounted volume is blocking rebuild."
    elif not ok and stale_lock:
        summary = f"{summary} Likely cause: stale generate lock is present without an active worker thread."
    details = {
        "summary": freshness.get("summary"),
        "reason_codes": freshness.get("reason_codes") or [],
        "sampled_frame_integrity": freshness.get("sampled_frame_integrity") or {},
        "artifact_signature": freshness.get("artifact_signature"),
        "code_dependency_hash": freshness.get("code_dependency_hash"),
        "source_data_hash": freshness.get("source_data_hash"),
        "lock_present": lock_snapshot.get("lock_present"),
        "lock_age_seconds": lock_snapshot.get("lock_age_seconds"),
        "thread_alive": lock_snapshot.get("thread_alive"),
    }
    if likely_cause:
        details["likely_cause"] = likely_cause
    if likely_cause == "low_space_volume":
        details["storage_report"] = low_space_ctx["storage_report"]

    return _response(
        ok,
        "generated-artifact-sync",
        summary,
        details,
    )
