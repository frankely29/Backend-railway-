from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict


def _normalized_json_hash(payload: Any) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256_text(normalized)


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


def safe_file_meta(path: Path) -> dict:
    meta: Dict[str, Any] = {"name": path.name}
    try:
        stat = path.stat()
        meta["size"] = int(stat.st_size)
        meta["mtime"] = int(stat.st_mtime)
    except Exception:
        meta["size"] = None
        meta["mtime"] = None
    return meta


def build_source_inventory_signature(data_dir: Path) -> dict:
    parquet_files = sorted([p for p in data_dir.glob("*.parquet") if p.is_file()], key=lambda p: p.name)
    parquet_meta = [safe_file_meta(path) for path in parquet_files]
    parquet_inventory_hash = _normalized_json_hash(parquet_meta)

    zones_path = data_dir / "taxi_zones.geojson"
    zones_meta: Dict[str, Any] | None = None
    zones_hash: str | None = None
    if zones_path.exists() and zones_path.is_file():
        zones_meta = safe_file_meta(zones_path)
        zones_hash = sha256_file(zones_path)
        zones_meta["hash"] = zones_hash

    source_data_hash = _normalized_json_hash(
        {
            "parquet_inventory_hash": parquet_inventory_hash,
            "zones_hash": zones_hash,
            "zones_geojson": zones_meta,
        }
    )
    return {
        "parquet_files": parquet_meta,
        "parquet_inventory_hash": parquet_inventory_hash,
        "zones_geojson": zones_meta,
        "zones_hash": zones_hash,
        "source_data_hash": source_data_hash,
    }


def build_code_dependency_signature(repo_root: Path) -> dict:
    files = [
        "build_hotspot.py",
        "zone_earnings_engine.py",
        "zone_mode_profiles.py",
        "zone_geometry_metrics.py",
        "build_day_tendency.py",
        "main.py",
    ]
    dependency_files = []
    for name in files:
        path = repo_root / name
        if not path.exists() or not path.is_file():
            continue
        dependency_files.append({"name": name, "hash": sha256_file(path)})

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

    signature_payload = {
        "artifact_schema_version": artifact_schema_version,
        "code_dependency_hash": code_dependencies["code_dependency_hash"],
        "source_data_hash": source_inventory["source_data_hash"],
        "bin_minutes": int(bin_minutes),
        "min_trips_per_window": int(min_trips_per_window),
        "expected_visible_profiles_live": expected_visible_profiles_live,
        "expected_comparison_profiles": expected_comparison_profiles,
        "expected_default_citywide_profile": expected_default_citywide_profile,
    }
    artifact_signature = _normalized_json_hash(signature_payload)

    return {
        **signature_payload,
        "artifact_signature": artifact_signature,
        "source_inventory": source_inventory,
        "code_dependencies": code_dependencies,
    }


def load_existing_artifact_manifest(frames_dir: Path) -> dict | None:
    manifest_path = frames_dir / "scoring_shadow_manifest.json"
    try:
        if not manifest_path.exists() or not manifest_path.is_file():
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        return None
    except Exception:
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
        timeline = payload.get("timeline")
        if isinstance(timeline, list):
            return timeline
        frames = payload.get("frames")
        if isinstance(frames, list):
            return frames
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


def sample_frame_integrity(frames_dir: Path) -> dict:
    timeline_items = _timeline_items(frames_dir)
    if not timeline_items:
        return {
            "sampled_frame_indices": [],
            "sampled_feature_count": 0,
            "frame_has_v3_fields": False,
            "frame_has_density_fields": False,
            "frame_has_trap_fields": False,
        }

    indices = sorted(set([0, len(timeline_items) // 2, len(timeline_items) - 1]))
    sampled_features: list[Dict[str, Any]] = []

    for idx in indices:
        frame_path = frames_dir / f"frame_{idx:06d}.json"
        if not frame_path.exists() or not frame_path.is_file():
            continue
        try:
            payload = json.loads(frame_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        sampled_features.extend(_frame_features(payload))

    v3_fields = [
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
    ]
    trap_fields = [
        "long_trip_share_20plus_shadow",
        "same_zone_retention_penalty_n_shadow",
    ]

    def _all_features_have(required: list[str]) -> bool:
        if not sampled_features:
            return False
        for feature in sampled_features:
            props = feature.get("properties") if isinstance(feature, dict) else None
            if not isinstance(props, dict):
                return False
            if any(field not in props for field in required):
                return False
        return True

    return {
        "sampled_frame_indices": indices,
        "sampled_feature_count": len(sampled_features),
        "frame_has_v3_fields": _all_features_have(v3_fields),
        "frame_has_density_fields": _all_features_have(density_fields),
        "frame_has_trap_fields": _all_features_have(trap_fields),
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

    timeline_path = frames_dir / "timeline.json"
    timeline_items = _timeline_items(frames_dir)
    if not timeline_path.exists():
        reason_codes.append("timeline_missing")

    if not timeline_items:
        reason_codes.append("frame_files_missing")
    else:
        indices = sorted(set([0, len(timeline_items) // 2, len(timeline_items) - 1]))
        for idx in indices:
            frame_path = frames_dir / f"frame_{idx:06d}.json"
            if not frame_path.exists() or frame_path.stat().st_size <= 0:
                reason_codes.append("frame_files_missing")
                break

    sampled = sample_frame_integrity(frames_dir)
    if not sampled.get("frame_has_v3_fields"):
        reason_codes.append("sampled_frames_missing_v3_fields")
    if not sampled.get("frame_has_density_fields"):
        reason_codes.append("sampled_frames_missing_density_fields")
    if not sampled.get("frame_has_trap_fields"):
        reason_codes.append("sampled_frames_missing_trap_fields")

    if current_manifest is None:
        reason_codes.append("manifest_missing")
    else:
        if current_manifest.get("artifact_signature") != expected.get("artifact_signature"):
            reason_codes.append("artifact_signature_mismatch")
        if current_manifest.get("code_dependency_hash") != expected.get("code_dependency_hash"):
            reason_codes.append("code_dependency_hash_mismatch")
        if current_manifest.get("source_data_hash") != expected.get("source_data_hash"):
            reason_codes.append("source_data_hash_mismatch")
        if current_manifest.get("visible_profiles_live") != expected.get("expected_visible_profiles_live"):
            reason_codes.append("visible_profiles_mismatch")
        if current_manifest.get("default_citywide_profile") != expected.get("expected_default_citywide_profile"):
            reason_codes.append("default_citywide_profile_mismatch")

    fresh = len(reason_codes) == 0
    summary = (
        "Artifacts are fresh and aligned with code, source data, and expected rollout profiles."
        if fresh
        else f"Artifacts are stale ({', '.join(reason_codes)})."
    )

    deduped_reason_codes = list(dict.fromkeys(reason_codes))
    return {
        "fresh": fresh,
        "reason_codes": deduped_reason_codes,
        "expected": expected,
        "current_manifest": current_manifest,
        "sampled_frame_integrity": sampled,
        "summary": summary,
    }
