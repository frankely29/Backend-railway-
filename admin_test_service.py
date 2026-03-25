from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from admin_service import get_admin_pickup_logs, get_admin_police_reports, get_admin_summary
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary
from artifact_freshness import evaluate_artifact_freshness
from core import DB_BACKEND, _db_query_all, _db_query_one


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
    frames_dir = _frames_dir()
    timeline_path = frames_dir / "timeline.json"
    manifest_path = frames_dir / "scoring_shadow_manifest.json"

    backend_build_id = (os.environ.get("BACKEND_BUILD_ID") or "").strip()
    backend_release = (os.environ.get("BACKEND_RELEASE") or "").strip()
    timeline_present = timeline_path.exists() and timeline_path.stat().st_size > 0 if timeline_path.exists() else False
    manifest_present = manifest_path.exists() and manifest_path.stat().st_size > 0 if manifest_path.exists() else False
    identity_available = bool(backend_build_id or backend_release)

    return _response(
        identity_available,
        "build-sync",
        "Backend build identity available" if identity_available else "Backend build identity missing",
        {
            "backend_build_id": backend_build_id,
            "backend_release": backend_release,
            "frames_dir": str(frames_dir),
            "manifest_present": manifest_present,
            "timeline_present": timeline_present,
        },
    )


def test_timeline() -> Dict[str, Any]:
    frames_dir = _frames_dir()
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
    frames_dir = _frames_dir()
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


def test_score_manifest() -> Dict[str, Any]:
    frames_dir = _frames_dir()
    manifest_path = frames_dir / "scoring_shadow_manifest.json"
    manifest_present = manifest_path.exists() and manifest_path.stat().st_size > 0 if manifest_path.exists() else False

    expected_visible = [
        "citywide_v3",
        "manhattan_v3",
        "bronx_wash_heights_v3",
        "queens_v3",
        "brooklyn_v3",
        "staten_island_v3",
    ]

    freshness = evaluate_artifact_freshness(
        repo_root=_repo_root(),
        data_dir=_data_dir(),
        frames_dir=frames_dir,
        bin_minutes=int(os.environ.get("DEFAULT_BIN_MINUTES", "20")),
        min_trips_per_window=int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25")),
    )

    if not manifest_present or not freshness.get("fresh"):
        return _response(
            False,
            "score-manifest",
            "Generated artifacts are stale or missing.",
            {
                "manifest_path": str(manifest_path),
                "manifest_present": manifest_present,
                "visible_profiles_live": [],
                "default_citywide_profile": None,
                "mismatches": freshness.get("reason_codes") or ["manifest missing"],
            },
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _response(
            False,
            "score-manifest",
            "Scoring manifest does not match the expected v3 rollout.",
            {
                "manifest_path": str(manifest_path),
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
            "manifest_path": str(manifest_path),
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
    from main import _generate_lock_snapshot

    frames_dir = _frames_dir()
    timeline_path = frames_dir / "timeline.json"
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

    sampled_features: list[Dict[str, Any]] = []
    violations: list[str] = []

    for frame_idx in sampled_frame_indices:
        frame_path = frames_dir / f"frame_{frame_idx:06d}.json"
        if not frame_path.exists() or frame_path.stat().st_size == 0:
            violations.append(f"missing frame_{frame_idx:06d}.json")
            continue
        try:
            frame_payload = json.loads(frame_path.read_text(encoding="utf-8"))
        except Exception as exc:
            violations.append(f"unreadable frame_{frame_idx:06d}.json: {exc}")
            continue

        frame_features: list[Any] = []
        if isinstance(frame_payload, dict):
            polygons = frame_payload.get("polygons")
            if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
                frame_features = polygons.get("features") or []
            elif isinstance(frame_payload.get("features"), list):
                frame_features = frame_payload.get("features") or []

        for feature in frame_features:
            if isinstance(feature, dict):
                sampled_features.append(feature)

    for feature_idx, feature in enumerate(sampled_features):
        props = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(props, dict):
            violations.append(f"feature {feature_idx}: missing properties")
            continue

        for key in required_rating_fields + required_metric_fields:
            if key not in props:
                violations.append(f"feature {feature_idx}: missing {key}")

        for rating_key in required_rating_fields:
            value = props.get(rating_key)
            if value is not None and not (1 <= float(value) <= 100):
                violations.append(f"feature {feature_idx}: {rating_key} out of range [1,100]")

        for confidence_key in confidence_fields:
            value = props.get(confidence_key)
            if value is not None and not (0 <= float(value) <= 1):
                violations.append(f"feature {feature_idx}: {confidence_key} out of range [0,1]")

        area = props.get("zone_area_sq_miles_shadow")
        if area is not None and float(area) <= 0:
            violations.append(f"feature {feature_idx}: zone_area_sq_miles_shadow must be > 0")

        for density_key in ("pickups_per_sq_mile_now_shadow", "pickups_per_sq_mile_next_shadow"):
            density_value = props.get(density_key)
            if density_value is not None and float(density_value) < 0:
                violations.append(f"feature {feature_idx}: {density_key} must be >= 0")

        for share_key in (
            "long_trip_share_20plus_shadow",
            "same_zone_dropoff_share_shadow",
            "demand_density_now_n_shadow",
            "demand_density_next_n_shadow",
            "same_zone_retention_penalty_n_shadow",
        ):
            share_value = props.get(share_key)
            if share_value is not None and not (0 <= float(share_value) <= 1):
                violations.append(f"feature {feature_idx}: {share_key} out of range [0,1]")

    freshness = evaluate_artifact_freshness(
        repo_root=_repo_root(),
        data_dir=_data_dir(),
        frames_dir=frames_dir,
        bin_minutes=int(os.environ.get("DEFAULT_BIN_MINUTES", "20")),
        min_trips_per_window=int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25")),
    )
    sampled_integrity = freshness.get("sampled_frame_integrity") or {}
    stale_field_mismatch = (
        not sampled_integrity.get("frame_has_citywide_v3")
        or not sampled_integrity.get("frame_has_borough_v3_fields")
        or not sampled_integrity.get("frame_has_density_fields")
        or not sampled_integrity.get("frame_has_trap_fields")
    )
    lock_snapshot = _generate_lock_snapshot()
    stale_lock = bool(lock_snapshot.get("lock_present")) and not bool(lock_snapshot.get("thread_alive"))

    ok = bool(sampled_features) and len(violations) == 0 and not stale_field_mismatch
    return _response(
        ok,
        "score-frame-integrity",
        "Sampled frame features contain valid v3 score, density, and trap fields."
        if ok
        else (
            "Frames appear older than the deployed scoring code; rebuild may be blocked by a stale generate lock."
            if stale_field_mismatch and stale_lock
            else "Frames appear older than the deployed scoring code."
            if stale_field_mismatch
            else "Sampled frame features contain invalid or missing score fields."
        ),
        {
            "sampled_frame_indices": sampled_frame_indices,
            "sampled_feature_count": len(sampled_features),
            "violation_count": len(violations),
            "first_violations": violations[:10],
            "sampled_frame_integrity": sampled_integrity,
            "lock_present": lock_snapshot.get("lock_present"),
            "lock_age_seconds": lock_snapshot.get("lock_age_seconds"),
            "thread_alive": lock_snapshot.get("thread_alive"),
        },
    )

def test_generated_artifact_sync() -> Dict[str, Any]:
    from main import _generate_lock_snapshot

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
    summary = freshness.get("summary") or ("Generated frame artifacts match the deployed v3 code." if ok else "Generated frame artifacts are stale.")
    if not ok and stale_lock:
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
    if stale_lock:
        details["likely_cause"] = "stale_generate_lock"

    return _response(
        ok,
        "generated-artifact-sync",
        summary,
        details,
    )
