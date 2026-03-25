from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from admin_service import get_admin_pickup_logs, get_admin_police_reports, get_admin_summary
from admin_trips_service import get_admin_recent_trips, get_admin_trips_summary
from core import DB_BACKEND, _db_query_all, _db_query_one
from zone_earnings_engine import build_zone_earnings_shadow_sql
from zone_geometry_metrics import build_zone_geometry_metrics_rows
from zone_mode_profiles import ZONE_MODE_PROFILES
from artifact_freshness import evaluate_artifact_freshness


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
    if not manifest_path.exists():
        return _response(
            False,
            "score-manifest",
            "Generated artifacts are stale or missing.",
            {"manifest_path": str(manifest_path), "mismatches": ["Manifest file missing"]},
        )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _response(
            False,
            "score-manifest",
            "Scoring manifest does not match the expected v3 rollout.",
            {"manifest_path": str(manifest_path), "mismatches": [f"Manifest unreadable: {exc}"]},
        )

    expected_visible = [
        "citywide_v3",
        "manhattan_v3",
        "bronx_wash_heights_v3",
        "queens_v3",
        "brooklyn_v3",
        "staten_island_v3",
    ]
    expected_v2 = [
        "citywide_v2",
        "manhattan_v2",
        "bronx_wash_heights_v2",
        "queens_v2",
        "brooklyn_v2",
        "staten_island_v2",
    ]

    visible_profiles_live = manifest.get("visible_profiles_live")
    comparison_profiles = manifest.get("comparison_profiles")
    candidate_shadow_profiles = manifest.get("candidate_shadow_profiles")

    mismatches = []
    if manifest.get("engine_version") != "team-joseo-score-v2-final-live":
        mismatches.append("engine_version mismatch")
    if manifest.get("default_citywide_profile") != "citywide_v3":
        mismatches.append("default_citywide_profile mismatch")
    if manifest.get("all_profiles_live") is not True:
        mismatches.append("all_profiles_live must be true")
    if manifest.get("base_color_truth") != "tlc_hvfhv_earnings_opportunity":
        mismatches.append("base_color_truth mismatch")
    if visible_profiles_live != expected_visible:
        mismatches.append("visible_profiles_live mismatch")
    if not isinstance(comparison_profiles, list) or any(profile not in comparison_profiles for profile in expected_v2):
        mismatches.append("comparison_profiles missing required v2 profiles")
    if candidate_shadow_profiles not in (None, []):
        mismatches.append("candidate_shadow_profiles must be missing or empty")

    ok = len(mismatches) == 0
    return _response(
        ok,
        "score-manifest",
        "Scoring manifest matches the full v3 rollout." if ok else "Scoring manifest does not match the expected v3 rollout.",
        {
            "manifest_path": str(manifest_path),
            "visible_profiles_live": visible_profiles_live,
            "comparison_profiles": comparison_profiles,
            "default_citywide_profile": manifest.get("default_citywide_profile"),
            "all_profiles_live": manifest.get("all_profiles_live"),
            "base_color_truth": manifest.get("base_color_truth"),
            "mismatches": mismatches,
        },
    )


def test_score_sql_definitions() -> Dict[str, Any]:
    try:
        sql = build_zone_earnings_shadow_sql(
            ["/tmp/dummy.parquet"],
            bin_minutes=20,
            min_trips_per_window=25,
            profile=ZONE_MODE_PROFILES["citywide_v2"],
            citywide_v3_profile=ZONE_MODE_PROFILES["citywide_v3"],
            manhattan_profile=ZONE_MODE_PROFILES["manhattan_v2"],
            bronx_wash_heights_profile=ZONE_MODE_PROFILES["bronx_wash_heights_v2"],
            queens_profile=ZONE_MODE_PROFILES["queens_v2"],
            brooklyn_profile=ZONE_MODE_PROFILES["brooklyn_v2"],
            staten_island_profile=ZONE_MODE_PROFILES["staten_island_v2"],
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
            {"error": str(exc)},
        )

    checks = {
        "has_short_trip_gate": "trip_miles <= 3.0 AND trip_time <= 720.0" in sql,
        "has_long_trip_gate": "trip_time >= 1200" in sql,
        "has_retention_penalty_rank_asc": "ORDER BY same_zone_dropoff_share) AS same_zone_retention_penalty_rn" in sql,
        "has_retention_penalty_rank_desc": "ORDER BY same_zone_dropoff_share DESC) AS same_zone_retention_penalty_rn" in sql,
    }
    ok = checks["has_short_trip_gate"] and checks["has_long_trip_gate"] and checks["has_retention_penalty_rank_asc"] and not checks["has_retention_penalty_rank_desc"]
    return _response(
        ok,
        "score-sql-definitions",
        "Score SQL definitions match the intended trap-aware logic."
        if ok
        else "Score SQL definitions do not match the intended trap-aware logic.",
        {**checks, "freshness_scope": "This test validates code SQL definitions only, not persisted frame freshness."},
    )


def test_zone_geometry_metrics() -> Dict[str, Any]:
    zones_geojson_path = _data_dir() / "taxi_zones.geojson"
    if not zones_geojson_path.exists():
        return _response(
            False,
            "zone-geometry-metrics",
            "Zone geometry metrics are missing or invalid.",
            {"zones_geojson_path": str(zones_geojson_path), "error": "taxi_zones.geojson missing"},
        )

    try:
        rows = build_zone_geometry_metrics_rows(zones_geojson_path)
    except Exception as exc:
        return _response(
            False,
            "zone-geometry-metrics",
            "Zone geometry metrics are missing or invalid.",
            {"zones_geojson_path": str(zones_geojson_path), "error": str(exc)},
        )

    areas = [float(row.get("zone_area_sq_miles")) for row in rows if row.get("zone_area_sq_miles") is not None]
    positive_areas = [value for value in areas if value > 0]
    null_or_zero_area_rows = len(rows) - len(positive_areas)

    median_area_sq_miles = None
    if positive_areas:
        ordered = sorted(positive_areas)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 0:
            median_area_sq_miles = (ordered[mid - 1] + ordered[mid]) / 2.0
        else:
            median_area_sq_miles = ordered[mid]

    min_area_sq_miles = min(positive_areas) if positive_areas else None
    max_area_sq_miles = max(positive_areas) if positive_areas else None

    ok = (
        len(rows) > 200
        and len(positive_areas) > 200
        and null_or_zero_area_rows <= 10
        and min_area_sq_miles is not None
        and min_area_sq_miles > 0
        and max_area_sq_miles is not None
        and min_area_sq_miles < max_area_sq_miles
    )
    return _response(
        ok,
        "zone-geometry-metrics",
        "Zone geometry metrics look valid." if ok else "Zone geometry metrics are missing or invalid.",
        {
            "zones_geojson_path": str(zones_geojson_path),
            "total_rows": len(rows),
            "positive_area_rows": len(positive_areas),
            "null_or_zero_area_rows": null_or_zero_area_rows,
            "min_area_sq_miles": min_area_sq_miles,
            "median_area_sq_miles": median_area_sq_miles,
            "max_area_sq_miles": max_area_sq_miles,
        },
    )


def test_score_frame_integrity() -> Dict[str, Any]:
    frames_dir = _frames_dir()
    timeline_path = frames_dir / "timeline.json"
    if not timeline_path.exists():
        return _response(
            False,
            "score-frame-integrity",
            "Frames appear older than the deployed scoring code.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": [f"timeline.json missing at {timeline_path}"],
            },
        )
    try:
        timeline_payload = json.loads(timeline_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _response(
            False,
            "score-frame-integrity",
            "Frames appear older than the deployed scoring code.",
            {
                "sampled_frame_indices": [],
                "sampled_feature_count": 0,
                "violation_count": 1,
                "first_violations": [f"timeline.json unreadable: {exc}"],
            },
        )

    timeline_items: list[Any] = []
    if isinstance(timeline_payload, dict):
        if isinstance(timeline_payload.get("timeline"), list):
            timeline_items = timeline_payload.get("timeline") or []
        elif isinstance(timeline_payload.get("frames"), list):
            timeline_items = timeline_payload.get("frames") or []
    elif isinstance(timeline_payload, list):
        timeline_items = timeline_payload

    if not timeline_items:
        return _response(
            False,
            "score-frame-integrity",
            "Frames appear older than the deployed scoring code.",
            {"sampled_frame_indices": [], "sampled_feature_count": 0, "violation_count": 1, "first_violations": ["timeline has no frame entries"]},
        )

    indices = sorted(set([0, len(timeline_items) // 2, len(timeline_items) - 1]))
    features: list[Dict[str, Any]] = []
    violations: list[str] = []
    v3_rating_fields = [
        "earnings_shadow_rating_citywide_v3",
        "earnings_shadow_rating_manhattan_v3",
        "earnings_shadow_rating_bronx_wash_heights_v3",
        "earnings_shadow_rating_queens_v3",
        "earnings_shadow_rating_brooklyn_v3",
        "earnings_shadow_rating_staten_island_v3",
    ]
    density_fields = [
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

    for idx in indices:
        frame_path = frames_dir / f"frame_{idx:06d}.json"
        if not frame_path.exists():
            violations.append(f"missing frame file for index {idx}: {frame_path.name}")
            continue
        try:
            frame_payload = json.loads(frame_path.read_text(encoding="utf-8"))
        except Exception as exc:
            violations.append(f"unreadable frame file for index {idx}: {exc}")
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
                features.append(feature)

    for i, feature in enumerate(features):
        props = feature.get("properties") if isinstance(feature, dict) else None
        if not isinstance(props, dict):
            violations.append(f"feature {i}: properties missing")
            continue
        for key in ("LocationID", "rating", "bucket"):
            if key not in props:
                violations.append(f"feature {i}: missing properties.{key}")
        style = props.get("style")
        if not isinstance(style, dict) or "fillColor" not in style:
            violations.append(f"feature {i}: missing properties.style.fillColor")
        for key in v3_rating_fields + density_fields:
            if key not in props:
                violations.append(f"feature {i}: missing properties.{key}")

        if (area := props.get("zone_area_sq_miles_shadow")) is not None and float(area) <= 0:
            violations.append(f"feature {i}: zone_area_sq_miles_shadow must be > 0")
        for nonneg in ("pickups_per_sq_mile_now_shadow", "pickups_per_sq_mile_next_shadow"):
            value = props.get(nonneg)
            if value is not None and float(value) < 0:
                violations.append(f"feature {i}: {nonneg} must be >= 0")
        for zero_one in (
            "long_trip_share_20plus_shadow",
            "same_zone_dropoff_share_shadow",
            "demand_density_now_n_shadow",
            "demand_density_next_n_shadow",
            "same_zone_retention_penalty_n_shadow",
        ):
            value = props.get(zero_one)
            if value is not None and not (0 <= float(value) <= 1):
                violations.append(f"feature {i}: {zero_one} out of [0,1]")
        for rating_field in v3_rating_fields:
            value = props.get(rating_field)
            if value is not None and not (1 <= float(value) <= 100):
                violations.append(f"feature {i}: {rating_field} out of [1,100]")
        for confidence_field in confidence_fields:
            value = props.get(confidence_field)
            if value is not None and not (0 <= float(value) <= 1):
                violations.append(f"feature {i}: {confidence_field} out of [0,1]")

    ok = bool(features) and not violations
    return _response(
        ok,
        "score-frame-integrity",
        "Sampled frame features contain valid v3 score, density, and trap fields."
        if ok
        else "Frames appear older than the deployed scoring code.",
        {
            "sampled_frame_indices": indices,
            "sampled_feature_count": len(features),
            "violation_count": len(violations),
            "first_violations": violations[:10],
        },
    )


def test_generated_artifact_sync() -> Dict[str, Any]:
    frames_dir = _frames_dir()
    data_dir = _data_dir()
    report = evaluate_artifact_freshness(
        repo_root=Path(__file__).resolve().parent,
        data_dir=data_dir,
        frames_dir=frames_dir,
        bin_minutes=int(os.environ.get("DEFAULT_BIN_MINUTES", "20")),
        min_trips_per_window=int(os.environ.get("DEFAULT_MIN_TRIPS_PER_WINDOW", "25")),
    )
    expected = report.get("expected") if isinstance(report, dict) else {}
    ok = bool(report.get("fresh"))
    return _response(
        ok,
        "generated-artifact-sync",
        "Generated frame artifacts match deployed code and source data."
        if ok
        else "Generated frame artifacts are stale and need regeneration.",
        {
            "summary": report.get("summary"),
            "reason_codes": report.get("reason_codes") or [],
            "sampled_frame_integrity": report.get("sampled_frame_integrity") or {},
            "artifact_signature": expected.get("artifact_signature"),
            "code_dependency_hash": expected.get("code_dependency_hash"),
            "source_data_hash": expected.get("source_data_hash"),
        },
    )
