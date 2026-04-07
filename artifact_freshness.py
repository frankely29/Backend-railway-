from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        return sha256_bytes(path.read_bytes())
    except Exception:
        return None


def _normalized_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _normalized_json_hash(payload: Any) -> str:
    return sha256_text(_normalized_json(payload))


def _safe_file_meta(path: Path) -> dict:
    meta: Dict[str, Any] = {"name": path.name, "size": None, "mtime": None}
    try:
        stat = path.stat()
        meta["size"] = int(stat.st_size)
        meta["mtime"] = int(stat.st_mtime)
    except Exception:
        pass
    return meta


def build_source_inventory_signature(data_dir: Path) -> dict:
    parquet_paths = sorted((p for p in data_dir.glob("*.parquet") if p.is_file()), key=lambda p: p.name)
    parquet_files = [_safe_file_meta(path) for path in parquet_paths]
    parquet_inventory_hash = _normalized_json_hash(parquet_files)

    zones_path = data_dir / "taxi_zones.geojson"
    zones_geojson: dict | None = None
    if zones_path.exists() and zones_path.is_file():
        zones_geojson = _safe_file_meta(zones_path)
        zones_geojson["hash"] = sha256_file(zones_path)

    source_data_hash = _normalized_json_hash(
        {
            "parquet_inventory_hash": parquet_inventory_hash,
            "zones_geojson": zones_geojson,
        }
    )

    return {
        "parquet_files": parquet_files,
        "parquet_inventory_hash": parquet_inventory_hash,
        "zones_geojson": zones_geojson,
        "source_data_hash": source_data_hash,
    }


def build_code_dependency_signature(repo_root: Path) -> dict:
    dependency_names = [
        "build_hotspot.py",
        "zone_earnings_engine.py",
        "zone_mode_profiles.py",
        "zone_geometry_metrics.py",
        "exact_history_feature_builder.py",
        "timeline_time_utils.py",
    ]
    dependency_files: list[dict[str, Any]] = []
    for name in dependency_names:
        candidate = repo_root / name
        if not candidate.exists() or not candidate.is_file():
            continue
        dependency_files.append({"name": name, "hash": sha256_file(candidate)})

    code_dependency_hash = _normalized_json_hash(dependency_files)
    return {
        "dependency_files": dependency_files,
        "code_dependency_hash": code_dependency_hash,
    }


def build_expected_artifact_signature(
    repo_root: Path,
    data_dir: Path,
    frames_dir: Path,
    bin_minutes: int,
    min_trips_per_window: int,
) -> dict:
    _ = frames_dir

    source_inventory = build_source_inventory_signature(data_dir)
    code_dependencies = build_code_dependency_signature(repo_root)

    expected_visible_profiles_live = [
        "citywide_v3",
        "manhattan_v3",
        "bronx_wash_heights_v3",
        "queens_v3",
        "brooklyn_v3",
        "staten_island_v3",
    ]
    expected_comparison_profiles = [
        "citywide_v2",
        "manhattan_v2",
        "bronx_wash_heights_v2",
        "queens_v2",
        "brooklyn_v2",
        "staten_island_v2",
    ]
    expected_default_citywide_profile = "citywide_v3"
    artifact_schema_version = "team-joseo-artifacts-v3-final"

    source_data_hash = source_inventory["source_data_hash"]
    code_dependency_hash = code_dependencies["code_dependency_hash"]

    signature_basis = {
        "artifact_schema_version": artifact_schema_version,
        "expected_visible_profiles_live": expected_visible_profiles_live,
        "expected_comparison_profiles": expected_comparison_profiles,
        "expected_default_citywide_profile": expected_default_citywide_profile,
        "source_data_hash": source_data_hash,
        "code_dependency_hash": code_dependency_hash,
        "bin_minutes": int(bin_minutes),
        "min_trips_per_window": int(min_trips_per_window),
    }
    artifact_signature = _normalized_json_hash(signature_basis)

    return {
        "artifact_signature": artifact_signature,
        "source_inventory": source_inventory,
        "code_dependencies": code_dependencies,
        "expected_visible_profiles_live": expected_visible_profiles_live,
        "expected_comparison_profiles": expected_comparison_profiles,
        "expected_default_citywide_profile": expected_default_citywide_profile,
        "artifact_schema_version": artifact_schema_version,
        "source_data_hash": source_data_hash,
        "code_dependency_hash": code_dependency_hash,
        "bin_minutes": int(bin_minutes),
        "min_trips_per_window": int(min_trips_per_window),
    }


def load_existing_artifact_manifest(frames_dir: Path) -> dict | None:
    try:
        from artifact_db_store import load_generated_artifact

        artifact = load_generated_artifact("scoring_shadow_manifest")
        if artifact and isinstance(artifact.get("payload"), dict):
            return artifact.get("payload")
    except Exception:
        pass

    manifest_path = frames_dir / "scoring_shadow_manifest.json"
    try:
        if not manifest_path.exists() or not manifest_path.is_file():
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _timeline_items(frames_dir: Path) -> list[Any]:
    timeline_path = frames_dir / "timeline.json"
    if not timeline_path.exists() or not timeline_path.is_file():
        return []

    try:
        payload = json.loads(timeline_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(payload, dict):
        if isinstance(payload.get("timeline"), list):
            return payload.get("timeline") or []
        if isinstance(payload.get("frames"), list):
            return payload.get("frames") or []
        return []
    if isinstance(payload, list):
        return payload
    return []


def _frame_features(frame_payload: Any) -> list[Dict[str, Any]]:
    if not isinstance(frame_payload, dict):
        return []

    polygons = frame_payload.get("polygons")
    if isinstance(polygons, dict) and isinstance(polygons.get("features"), list):
        return [f for f in polygons.get("features") or [] if isinstance(f, dict)]
    if isinstance(frame_payload.get("features"), list):
        return [f for f in frame_payload.get("features") or [] if isinstance(f, dict)]
    return []


def _load_month_manifest(data_dir: Path) -> dict | None:
    manifest_path = data_dir / "exact_history" / "month_manifest.json"
    try:
        if not manifest_path.exists() or not manifest_path.is_file() or manifest_path.stat().st_size <= 0:
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _resolve_monthly_mode_context(data_dir: Path) -> dict:
    manifest = _load_month_manifest(data_dir)
    months = manifest.get("months") if isinstance(manifest, dict) and isinstance(manifest.get("months"), dict) else {}
    available_month_keys = sorted([key for key in months.keys() if str(key).strip()])
    active_month_key = _resolve_active_month_key_for_freshness(available_month_keys)
    timeline_scope = str(manifest.get("timeline_scope") or "").strip().lower() if isinstance(manifest, dict) else ""
    monthly_mode = bool(
        timeline_scope == "monthly_exact_historical"
        or (data_dir / "exact_history" / "months").exists()
        or available_month_keys
    )
    return {
        "monthly_mode": monthly_mode,
        "manifest": manifest,
        "available_month_keys": available_month_keys,
        "active_month_key": active_month_key,
    }


def _resolve_active_month_key_for_freshness(available_month_keys: list[str]) -> str | None:
    valid = [mk for mk in sorted(available_month_keys) if _safe_parse_month_key(mk)]
    if not valid:
        return None
    target = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
    target_key = f"{int(target.year):04d}-{int(target.month):02d}"
    same_month = [mk for mk in valid if (_safe_parse_month_key(mk) or (0, 0))[1] == int(target.month)]
    if target_key in same_month:
        return target_key
    if same_month:
        return sorted(same_month)[-1]
    return valid[-1]


def _safe_parse_month_key(month_key: str) -> tuple[int, int] | None:
    mk = str(month_key or "").strip()
    if len(mk) != 7 or mk[4] != "-":
        return None
    try:
        year = int(mk[:4])
        month = int(mk[5:7])
    except Exception:
        return None
    if year < 2000 or month < 1 or month > 12:
        return None
    return year, month


def sample_frame_integrity(frames_dir: Path) -> dict:
    from build_hotspot import bucket_and_color_from_rating

    timeline_items = _timeline_items(frames_dir)
    if not timeline_items:
        return {
            "sampled_frame_indices": [],
            "sampled_frame_names": [],
            "sampled_feature_count": 0,
            "frame_has_citywide_v3": False,
            "frame_has_borough_v3_fields": False,
            "frame_has_density_fields": False,
            "frame_has_trap_fields": False,
            "frame_has_popup_metric_fields": False,
            "frame_has_rating_bucket_color_consistency": False,
        }

    sample_indices = sorted(set([0, len(timeline_items) // 2, len(timeline_items) - 1]))
    sampled_frame_names: list[str] = []
    sampled_features: list[Dict[str, Any]] = []

    for idx in sample_indices:
        frame_name = f"frame_{idx:06d}.json"
        frame_path = frames_dir / frame_name
        if not frame_path.exists() or not frame_path.is_file() or frame_path.stat().st_size <= 0:
            continue
        sampled_frame_names.append(frame_name)
        try:
            frame_payload = json.loads(frame_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        sampled_features.extend(_frame_features(frame_payload))

    citywide_required = ["earnings_shadow_rating_citywide_v3"]
    borough_required = [
        "earnings_shadow_rating_manhattan_v3",
        "earnings_shadow_rating_bronx_wash_heights_v3",
        "earnings_shadow_rating_queens_v3",
        "earnings_shadow_rating_brooklyn_v3",
        "earnings_shadow_rating_staten_island_v3",
    ]
    density_required = [
        "zone_area_sq_miles_shadow",
        "pickups_per_sq_mile_now_shadow",
        "pickups_per_sq_mile_next_shadow",
        "demand_density_now_n_shadow",
        "demand_density_next_n_shadow",
    ]
    trap_required = [
        "long_trip_share_20plus_shadow",
        "same_zone_dropoff_share_shadow",
        "same_zone_retention_penalty_n_shadow",
    ]
    popup_metric_required = [
        "pickups_now_shadow",
        "next_pickups_shadow",
        "zone_area_sq_miles_shadow",
        "pickups_per_sq_mile_now_shadow",
        "pickups_per_sq_mile_next_shadow",
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

    def _all_features_have(required_fields: list[str]) -> bool:
        if not sampled_features:
            return False
        for feature in sampled_features:
            props = feature.get("properties") if isinstance(feature, dict) else None
            if not isinstance(props, dict):
                return False
            if any(field not in props for field in required_fields):
                return False
        return True

    def _has_rating_bucket_color_consistency() -> bool:
        if not sampled_features:
            return False
        for feature in sampled_features:
            props = feature.get("properties") if isinstance(feature, dict) else None
            if not isinstance(props, dict):
                return False

            legacy_rating = props.get("rating")
            if legacy_rating is not None:
                try:
                    expected_bucket, expected_color = bucket_and_color_from_rating(int(legacy_rating))
                except Exception:
                    return False
                emitted_bucket = props.get("bucket")
                style = props.get("style") if isinstance(props.get("style"), dict) else {}
                emitted_color = style.get("fillColor")
                if emitted_bucket != expected_bucket or emitted_color != expected_color:
                    return False

            for family in shadow_rating_families:
                rating_field = f"earnings_shadow_rating_{family}"
                bucket_field = f"earnings_shadow_bucket_{family}"
                color_field = f"earnings_shadow_color_{family}"
                rating_value = props.get(rating_field)
                if rating_value is None:
                    continue
                try:
                    expected_bucket, expected_color = bucket_and_color_from_rating(int(rating_value))
                except Exception:
                    return False
                emitted_bucket = props.get(bucket_field)
                emitted_color = props.get(color_field)
                if emitted_bucket != expected_bucket or emitted_color != expected_color:
                    return False

        return True

    return {
        "sampled_frame_indices": sample_indices,
        "sampled_frame_names": sampled_frame_names,
        "sampled_feature_count": len(sampled_features),
        "frame_has_citywide_v3": _all_features_have(citywide_required),
        "frame_has_borough_v3_fields": _all_features_have(borough_required),
        "frame_has_density_fields": _all_features_have(density_required),
        "frame_has_trap_fields": _all_features_have(trap_required),
        "frame_has_popup_metric_fields": _all_features_have(popup_metric_required),
        "frame_has_rating_bucket_color_consistency": _has_rating_bucket_color_consistency(),
    }


def evaluate_artifact_freshness(
    repo_root: Path,
    data_dir: Path,
    frames_dir: Path,
    bin_minutes: int,
    min_trips_per_window: int,
) -> dict:
    expected = build_expected_artifact_signature(
        repo_root=repo_root,
        data_dir=data_dir,
        frames_dir=frames_dir,
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
    )
    current_manifest = load_existing_artifact_manifest(frames_dir)

    reason_codes: list[str] = []
    monthly_context = _resolve_monthly_mode_context(data_dir)
    monthly_mode = bool(monthly_context.get("monthly_mode"))

    sampled: dict[str, Any]
    if monthly_mode:
        active_month_key = str(monthly_context.get("active_month_key") or "").strip()
        manifest = monthly_context.get("manifest")
        months = manifest.get("months") if isinstance(manifest, dict) and isinstance(manifest.get("months"), dict) else {}
        month_manifest_entry = months.get(active_month_key) if active_month_key else None
        month_manifest_present = bool(manifest and isinstance(manifest, dict))
        if not month_manifest_present:
            reason_codes.append("month_manifest_missing")
        if not active_month_key:
            reason_codes.append("active_month_missing")
        month_dir = data_dir / "exact_history" / "months" / active_month_key if active_month_key else None
        month_timeline_path = month_dir / "timeline.json" if month_dir else None
        month_store_path = month_dir / "exact_shadow.duckdb" if month_dir else None
        month_build_meta_path = month_dir / "build_meta.json" if month_dir else None
        month_timeline_present = bool(
            month_timeline_path
            and month_timeline_path.exists()
            and month_timeline_path.is_file()
            and month_timeline_path.stat().st_size > 0
        )
        month_store_present = bool(
            month_store_path
            and month_store_path.exists()
            and month_store_path.is_file()
            and month_store_path.stat().st_size > 0
        )
        month_frame_cache_dir = month_dir / "frame_cache" if month_dir else None
        month_frame_cache_present = bool(month_frame_cache_dir and month_frame_cache_dir.exists() and month_frame_cache_dir.is_dir())
        month_build_meta: Dict[str, Any] | None = None
        if month_build_meta_path and month_build_meta_path.exists() and month_build_meta_path.is_file() and month_build_meta_path.stat().st_size > 0:
            try:
                payload = json.loads(month_build_meta_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    month_build_meta = payload
            except Exception:
                month_build_meta = None
        month_build_meta_present = bool(month_build_meta)
        if not month_timeline_present:
            reason_codes.append("active_month_timeline_missing")
        source_of_truth = str((month_build_meta or {}).get("source_of_truth") or "").strip() or None
        month_live_ready = bool(month_timeline_present and month_frame_cache_present)
        authoritative_fresh = False
        code_match = False
        source_match = False
        artifact_match = False
        if month_build_meta_present:
            code_match = month_build_meta.get("code_dependency_hash") == expected.get("code_dependency_hash")
            source_match = month_build_meta.get("source_data_hash") == expected.get("source_data_hash")
            artifact_match = month_build_meta.get("artifact_signature") == expected.get("artifact_signature")
            authoritative_fresh = bool(month_timeline_present and code_match and source_match and artifact_match)
            if source_of_truth == "exact_store":
                authoritative_fresh = bool(authoritative_fresh and month_store_present)
        if not month_store_present and source_of_truth != "parquet_live":
            reason_codes.append("active_month_store_missing")
        if not month_build_meta_present:
            reason_codes.append("active_month_build_meta_missing")
        if active_month_key and not month_manifest_entry:
            reason_codes.append("active_month_manifest_entry_missing")
        if month_build_meta_present and source_of_truth != "parquet_live":
            if not code_match:
                reason_codes.append("active_month_code_dependency_hash_mismatch")
            if not source_match:
                reason_codes.append("active_month_source_data_hash_mismatch")
            if not artifact_match:
                reason_codes.append("active_month_artifact_signature_mismatch")
        parquet_live_authoritative = bool(
            source_of_truth == "parquet_live"
            and authoritative_fresh
            and month_build_meta_present
            and code_match
            and source_match
            and artifact_match
            and month_timeline_present
            and month_live_ready
        )
        sampled = {
            "mode": "monthly_exact_historical",
            "active_month_key": active_month_key or None,
            "month_manifest_present": month_manifest_present,
            "active_month_timeline_present": month_timeline_present,
            "active_month_store_present": month_store_present,
            "active_month_live_ready": month_live_ready,
            "active_month_build_meta_present": month_build_meta_present,
            "active_month_source_of_truth": source_of_truth,
            "active_month_authoritative_fresh": authoritative_fresh,
            "active_month_signature_matches_code": code_match,
            "active_month_signature_matches_source": source_match,
            "active_month_artifact_signature_matches": artifact_match,
            "active_month_parquet_live_authoritative": parquet_live_authoritative,
            "active_month_build_meta_artifact_signature": (month_build_meta or {}).get("artifact_signature"),
            "active_month_expected_artifact_signature": expected.get("artifact_signature"),
            "legacy_frame_file_sampling_skipped": True,
        }
    else:
        timeline_path = frames_dir / "timeline.json"
        if not timeline_path.exists() or timeline_path.stat().st_size <= 0:
            reason_codes.append("timeline_missing")

        timeline_items = _timeline_items(frames_dir)
        frame_glob_files = sorted(frames_dir.glob("frame_*.json")) if frames_dir.exists() else []
        if not timeline_items or not frame_glob_files:
            reason_codes.append("frame_files_missing")

        sampled = sample_frame_integrity(frames_dir)
        if not sampled.get("frame_has_citywide_v3") or not sampled.get("frame_has_borough_v3_fields"):
            reason_codes.append("sampled_frames_missing_v3_fields")
        if not sampled.get("frame_has_density_fields"):
            reason_codes.append("sampled_frames_missing_density_fields")
        if not sampled.get("frame_has_trap_fields"):
            reason_codes.append("sampled_frames_missing_trap_fields")
        if not sampled.get("frame_has_popup_metric_fields"):
            reason_codes.append("sampled_frames_missing_popup_metric_fields")
        if not sampled.get("frame_has_rating_bucket_color_consistency"):
            reason_codes.append("sampled_frames_bucket_color_mismatch")

    parquet_live_authoritative = bool(sampled.get("active_month_parquet_live_authoritative")) if monthly_mode else False
    if not parquet_live_authoritative:
        if current_manifest is None:
            reason_codes.append("manifest_missing")
        else:
            if current_manifest.get("visible_profiles_live") != expected.get("expected_visible_profiles_live"):
                reason_codes.append("visible_profiles_mismatch")
            if current_manifest.get("default_citywide_profile") != expected.get("expected_default_citywide_profile"):
                reason_codes.append("default_citywide_profile_mismatch")
            if current_manifest.get("code_dependency_hash") != expected.get("code_dependency_hash"):
                reason_codes.append("code_dependency_hash_mismatch")
            if current_manifest.get("source_data_hash") != expected.get("source_data_hash"):
                reason_codes.append("source_data_hash_mismatch")
            if current_manifest.get("artifact_signature") != expected.get("artifact_signature"):
                reason_codes.append("artifact_signature_mismatch")
    else:
        reason_codes = [
            code
            for code in reason_codes
            if code
            not in {
                "active_month_store_missing",
                "code_dependency_hash_mismatch",
                "artifact_signature_mismatch",
            }
        ]

    reason_codes = list(dict.fromkeys(reason_codes))
    fresh = len(reason_codes) == 0
    summary = (
        "Artifacts are fresh and aligned with source/code signatures."
        if fresh
        else f"Artifacts are stale: {', '.join(reason_codes)}"
    )

    return {
        "fresh": fresh,
        "summary": summary,
        "reason_codes": reason_codes,
        "mode": "monthly_exact_historical" if monthly_mode else "legacy_frames",
        "expected": expected,
        "current_manifest": current_manifest,
        "sampled_frame_integrity": sampled,
        "artifact_signature": expected.get("artifact_signature"),
        "code_dependency_hash": expected.get("code_dependency_hash"),
        "source_data_hash": expected.get("source_data_hash"),
    }
