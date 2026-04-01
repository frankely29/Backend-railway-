from __future__ import annotations

import math
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


_MB = 1024 * 1024


def _safe_size(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        if path.is_file():
            return int(path.stat().st_size)
        if path.is_dir():
            total = 0
            for child in path.rglob("*"):
                try:
                    if child.is_file():
                        total += int(child.stat().st_size)
                except Exception:
                    continue
            return total
    except Exception:
        return 0
    return 0


def _stale_temp_build_dirs(data_dir: Path) -> List[Path]:
    dirs: List[Path] = []
    for pattern in ("build_*", "tmp_build_*", "frames.__building__*"):
        for candidate in data_dir.glob(pattern):
            if candidate.is_dir() and candidate.name != "frames.__building__":
                dirs.append(candidate)
    return sorted(dirs)


def _stale_temp_root_build_dirs() -> List[Path]:
    temp_root = Path(os.environ.get("ARTIFACT_BUILD_TMP_DIR", "/tmp/tlc_artifact_build"))
    if not temp_root.exists() or not temp_root.is_dir():
        return []
    cutoff_unix = time.time() - (6 * 3600)
    stale: List[Path] = []
    for candidate in temp_root.glob("build_*"):
        if not candidate.is_dir():
            continue
        try:
            if candidate.stat().st_mtime >= cutoff_unix:
                continue
        except Exception:
            continue
        stale.append(candidate)
    return sorted(stale)


def _cleanup_candidates(data_dir: Path, frames_dir: Path) -> List[Path]:
    # Safety: parquet files are source-of-truth raw data and must never be auto-deleted.
    # Safety: live frame_*.json and taxi_zones.geojson must never be cleanup targets.
    # Cleanup in this module is intentionally restricted to temp/build leftovers only.
    candidates: List[Path] = [
        data_dir / "duckdb_tmp",
        data_dir / "frames.__building__",
    ]
    candidates.extend(_stale_temp_build_dirs(data_dir))
    # Safety: temp-root cleanup only targets stale build_* directories.
    # Never target parquet files, frame_*.json, or taxi_zones.geojson.
    candidates.extend(_stale_temp_root_build_dirs())
    _ = frames_dir
    return candidates


def _sum_sizes(paths: Iterable[Path]) -> int:
    return sum(_safe_size(p) for p in paths)


def _is_build_tmp_on_data_volume(data_dir: Path, build_tmp_dir: Path) -> bool:
    try:
        data_resolved = data_dir.resolve()
        build_resolved = build_tmp_dir.resolve()
        if build_resolved == data_resolved or data_resolved in build_resolved.parents:
            return True
    except Exception:
        pass

    try:
        return int(os.stat(build_tmp_dir).st_dev) == int(os.stat(data_dir).st_dev)
    except Exception:
        return False


def get_artifact_storage_report(data_dir: Path, frames_dir: Path) -> Dict[str, Any]:
    usage = shutil.disk_usage(str(data_dir))
    parquet_files = sorted(data_dir.glob("*.parquet"))
    frames_dir_bytes = _safe_size(frames_dir)
    build_tmp_dir = Path(os.environ.get("ARTIFACT_BUILD_TMP_DIR", "/tmp/tlc_artifact_build"))
    build_tmp_on_data_volume = _is_build_tmp_on_data_volume(data_dir, build_tmp_dir)
    if build_tmp_on_data_volume:
        headroom_model = "stage_on_volume"
        recommended_free_bytes = max(256 * _MB, frames_dir_bytes + 256 * _MB)
    else:
        headroom_model = "publish_only"
        recommended_free_bytes = max(
            256 * _MB,
            min(512 * _MB, int(math.ceil(frames_dir_bytes * 0.10))),
        )
    free_bytes = int(usage.free)
    can_stage_rebuild = free_bytes >= recommended_free_bytes

    cleanup_candidates = [str(p) for p in _cleanup_candidates(data_dir, frames_dir) if p.exists()]
    return {
        "data_dir": str(data_dir),
        "frames_dir": str(frames_dir),
        "build_tmp_dir": str(build_tmp_dir),
        "build_tmp_on_data_volume": build_tmp_on_data_volume,
        "headroom_model": headroom_model,
        "disk_total_bytes": int(usage.total),
        "disk_used_bytes": int(usage.used),
        "disk_free_bytes": free_bytes,
        "disk_percent_used": round((usage.used / usage.total) * 100.0, 2) if usage.total else 0.0,
        "parquet_bytes": _sum_sizes(parquet_files),
        "frames_dir_bytes": frames_dir_bytes,
        "day_tendency_bytes": _safe_size(data_dir / "day_tendency"),
        "community_db_bytes": _safe_size(data_dir / "community_v2.db"),
        "zones_geojson_bytes": _safe_size(data_dir / "taxi_zones.geojson"),
        "old_volume_duckdb_tmp_bytes": _safe_size(data_dir / "duckdb_tmp"),
        "old_volume_stage_dir_bytes": _safe_size(data_dir / "frames.__building__"),
        "legacy_root_artifacts_bytes": 0,
        "cleanup_candidates": cleanup_candidates,
        "low_space": not can_stage_rebuild,
        "can_stage_rebuild": can_stage_rebuild,
        "recommended_free_bytes_for_rebuild": int(recommended_free_bytes),
    }


def cleanup_artifact_storage(data_dir: Path, frames_dir: Path) -> Dict[str, Any]:
    before = get_artifact_storage_report(data_dir, frames_dir)
    removed_paths: List[str] = []
    bytes_freed_estimate = 0

    for candidate in _cleanup_candidates(data_dir, frames_dir):
        if not candidate.exists():
            continue
        size_before = _safe_size(candidate)
        try:
            if candidate.is_dir():
                shutil.rmtree(candidate, ignore_errors=True)
            else:
                candidate.unlink(missing_ok=True)
        except Exception:
            continue
        if not candidate.exists():
            removed_paths.append(str(candidate))
            bytes_freed_estimate += size_before

    after = get_artifact_storage_report(data_dir, frames_dir)
    return {
        "before_report": before,
        "after_report": after,
        "removed_paths": removed_paths,
        "removed_count": len(removed_paths),
        "bytes_freed_estimate": int(bytes_freed_estimate),
    }
