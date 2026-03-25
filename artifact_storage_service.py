from __future__ import annotations

import shutil
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


def _legacy_candidates(data_dir: Path) -> List[Path]:
    candidates: List[Path] = []
    candidates.extend(sorted(data_dir.glob("frame_*.json")))
    for name in ("timeline.json", "scoring_shadow_manifest.json", "hotspots_20min.json"):
        p = data_dir / name
        if p.exists():
            candidates.append(p)
    return candidates


def _cleanup_candidates(data_dir: Path, frames_dir: Path) -> List[Path]:
    candidates: List[Path] = [
        data_dir / "duckdb_tmp",
        data_dir / "frames.__building__",
    ]
    if frames_dir.resolve() != data_dir.resolve():
        candidates.extend(_legacy_candidates(data_dir))
    return candidates


def _sum_sizes(paths: Iterable[Path]) -> int:
    return sum(_safe_size(p) for p in paths)


def get_artifact_storage_report(data_dir: Path, frames_dir: Path) -> Dict[str, Any]:
    usage = shutil.disk_usage(str(data_dir))
    parquet_files = sorted(data_dir.glob("*.parquet"))
    legacy_files = _legacy_candidates(data_dir) if frames_dir.resolve() != data_dir.resolve() else []
    recommended_free_bytes = max(256 * _MB, _safe_size(frames_dir) + 256 * _MB)
    free_bytes = int(usage.free)
    can_stage_rebuild = free_bytes >= recommended_free_bytes

    cleanup_candidates = [str(p) for p in _cleanup_candidates(data_dir, frames_dir) if p.exists()]
    return {
        "data_dir": str(data_dir),
        "frames_dir": str(frames_dir),
        "disk_total_bytes": int(usage.total),
        "disk_used_bytes": int(usage.used),
        "disk_free_bytes": free_bytes,
        "disk_percent_used": round((usage.used / usage.total) * 100.0, 2) if usage.total else 0.0,
        "parquet_bytes": _sum_sizes(parquet_files),
        "frames_dir_bytes": _safe_size(frames_dir),
        "day_tendency_bytes": _safe_size(data_dir / "day_tendency"),
        "community_db_bytes": _safe_size(data_dir / "community_v2.db"),
        "zones_geojson_bytes": _safe_size(data_dir / "taxi_zones.geojson"),
        "old_volume_duckdb_tmp_bytes": _safe_size(data_dir / "duckdb_tmp"),
        "old_volume_stage_dir_bytes": _safe_size(data_dir / "frames.__building__"),
        "legacy_root_artifacts_bytes": _sum_sizes(legacy_files),
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
