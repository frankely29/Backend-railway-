from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from artifact_freshness import build_expected_artifact_signature


def month_build_meta_path(exact_history_months_dir: Path, month_key: str) -> Path:
    return exact_history_months_dir / str(month_key).strip() / "build_meta.json"


def load_month_build_meta(exact_history_months_dir: Path, month_key: str) -> Dict[str, Any] | None:
    target = month_build_meta_path(exact_history_months_dir, month_key)
    try:
        if not target.exists() or not target.is_file() or target.stat().st_size <= 0:
            return None
        payload = json.loads(target.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def expected_active_month_signature(
    *,
    repo_root: Path,
    data_dir: Path,
    frames_dir: Path,
    bin_minutes: int,
    min_trips_per_window: int,
) -> Dict[str, Any]:
    return build_expected_artifact_signature(
        repo_root=repo_root,
        data_dir=data_dir,
        frames_dir=frames_dir,
        bin_minutes=int(bin_minutes),
        min_trips_per_window=int(min_trips_per_window),
    )


def active_month_freshness_report(
    *,
    month_key: str,
    exact_history_months_dir: Path,
    repo_root: Path,
    data_dir: Path,
    frames_dir: Path,
    bin_minutes: int,
    min_trips_per_window: int,
) -> Dict[str, Any]:
    mk = str(month_key or "").strip()
    month_dir = exact_history_months_dir / mk if mk else None
    timeline_path = month_dir / "timeline.json" if month_dir else None
    store_path = month_dir / "exact_shadow.duckdb" if month_dir else None

    timeline_present = bool(
        timeline_path and timeline_path.exists() and timeline_path.is_file() and timeline_path.stat().st_size > 0
    )
    store_present = bool(
        store_path and store_path.exists() and store_path.is_file() and store_path.stat().st_size > 0
    )

    build_meta = load_month_build_meta(exact_history_months_dir, mk) if mk else None
    expected = expected_active_month_signature(
        repo_root=repo_root,
        data_dir=data_dir,
        frames_dir=frames_dir,
        bin_minutes=bin_minutes,
        min_trips_per_window=min_trips_per_window,
    )

    source_of_truth = str((build_meta or {}).get("source_of_truth") or "").strip() or None
    code_match = bool(build_meta) and build_meta.get("code_dependency_hash") == expected.get("code_dependency_hash")
    source_match = bool(build_meta) and build_meta.get("source_data_hash") == expected.get("source_data_hash")
    artifact_match = bool(build_meta) and build_meta.get("artifact_signature") == expected.get("artifact_signature")
    common_signature_match = bool(timeline_present and build_meta and code_match and source_match and artifact_match)
    parquet_file_presence = True
    parquet_sources = (build_meta or {}).get("source_parquet_files")
    if source_of_truth == "parquet_live":
        parquet_file_presence = bool(isinstance(parquet_sources, list) and parquet_sources)
        if parquet_file_presence:
            parquet_file_presence = all((Path(data_dir) / str(name)).exists() for name in parquet_sources)
    if source_of_truth == "exact_store":
        signature_match = bool(common_signature_match and store_present)
        authoritative_kind = "exact_store"
    elif source_of_truth == "parquet_live":
        signature_match = bool(common_signature_match and parquet_file_presence)
        authoritative_kind = "parquet_live"
    else:
        signature_match = False
        authoritative_kind = None

    return {
        "month_key": mk or None,
        "timeline_present": timeline_present,
        "store_present": store_present,
        "source_of_truth": source_of_truth,
        "build_meta_present": bool(build_meta),
        "build_meta": build_meta,
        "expected": expected,
        "signature_match": signature_match,
        "authoritative_fresh": signature_match,
        "authoritative_kind": authoritative_kind,
        "source_parquet_files_present": bool(parquet_file_presence),
        "code_dependency_hash_match": bool(code_match),
        "source_data_hash_match": bool(source_match),
        "artifact_signature_match": bool(artifact_match),
    }
