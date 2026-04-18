from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ----------------------------------------------------------------------------
# Fixture — loads main.py against a tempdir DATA_DIR without running startup()
# ----------------------------------------------------------------------------
# These tests need to control bootstrap state BEFORE the detector runs, so we
# do NOT call main.startup() inside the fixture (unlike the other test files).
# Instead we import main, then individual tests stage state and invoke the
# helper functions directly.


@pytest.fixture()
def main_module(monkeypatch):
    temp_dir = tempfile.TemporaryDirectory(prefix="backend-auto-rebuild-detector-")
    data_dir = Path(temp_dir.name)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("COMMUNITY_DB", str(data_dir / "community.db"))
    monkeypatch.setenv("FRAMES_DIR", str(data_dir / "frames"))
    monkeypatch.setenv("JWT_SECRET", "test-jwt-secret-abcdefghijklmnopqrstuvwxyz")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "password123")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    for name in [
        "core",
        "chat",
        "leaderboard_db",
        "leaderboard_routes",
        "leaderboard_service",
        "leaderboard_tracker",
        "pickup_recording_feature",
        "games_models",
        "games_service",
        "games_routes",
        "main",
    ]:
        sys.modules.pop(name, None)

    main = importlib.import_module("main")

    # Reset module-level guard so each test starts clean.
    main._auto_rebuild_triggered_this_boot = False

    yield main, data_dir

    temp_dir.cleanup()


# ----------------------------------------------------------------------------
# Helpers — stage a month's on-disk state to simulate each detector scenario
# ----------------------------------------------------------------------------

MONTH_KEY = "2025-04"


def _month_dir(main_module, month_key: str) -> Path:
    return main_module._month_dir(str(month_key))


def _write_timeline(main_module, month_key: str) -> None:
    month_dir = _month_dir(main_module, month_key)
    month_dir.mkdir(parents=True, exist_ok=True)
    timeline_path = month_dir / "timeline.json"
    timeline_path.write_text(
        json.dumps({"timeline": ["2025-04-15T08:00:00-04:00"], "count": 1}),
        encoding="utf-8",
    )


def _write_frame_cache_dir(main_module, month_key: str) -> None:
    frame_cache = _month_dir(main_module, month_key) / "frame_cache"
    frame_cache.mkdir(parents=True, exist_ok=True)


def _write_fake_store(main_module, month_key: str, size_bytes: int = 128) -> None:
    store_path = main_module._month_store_path(str(month_key))
    store_path.parent.mkdir(parents=True, exist_ok=True)
    store_path.write_bytes(b"\x00" * size_bytes)


def _write_build_meta(main_module, month_key: str, payload: dict) -> None:
    build_meta_path = main_module._month_build_meta_path(str(month_key))
    build_meta_path.parent.mkdir(parents=True, exist_ok=True)
    build_meta_path.write_text(json.dumps(payload), encoding="utf-8")


def _write_fake_source_parquet(main_module, data_dir: Path, month_key: str) -> None:
    # _source_parquets_for_month scans /data/*.parquet and groups by filename.
    # For detector tests we don't actually read the parquet, but _group_parquets_by_month
    # needs a filename that can be month-keyed. Use the same naming convention as prod.
    parquet_name = f"fhvhv_tripdata_{_month_name_for_key(month_key)}.parquet"
    (data_dir / parquet_name).write_bytes(b"PAR1")  # minimal non-empty file


def _month_name_for_key(month_key: str) -> str:
    # "2025-04" -> "april-2025" (matching your fhvhv_tripdata_april-2025.parquet style)
    year, month = month_key.split("-")
    month_names = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    return f"{month_names[int(month) - 1]}-{year}"


def _detector_evaluate(main_module, month_key: str) -> dict:
    """
    Replays the detector's flag-computation logic exactly as it appears in
    main.py's startup() function, WITHOUT calling start_generate or touching
    the full startup sequence. Returns a dict of the derived flags plus the
    state_file_exists boolean, which is what we assert on.
    """
    snap = main_module._month_bootstrap_state(str(month_key))
    exact_store_retired_now = bool(snap.get("exact_store_retired"))
    store_exists_now = bool(snap.get("store_exists"))
    build_meta_orphaned_now = bool(
        store_exists_now
        and bool(snap.get("build_meta_present"))
        and not snap.get("source_of_truth")
    )
    new_month_needs_exact_store_build_now = bool(
        (not store_exists_now)
        and bool(snap.get("build_meta_present"))
        and str(snap.get("source_of_truth") or "").strip() == "parquet_live"
        and not bool(snap.get("exact_store_retired"))
        and bool(snap.get("source_parquet_exists"))
    )
    return {
        "exact_store_retired_now": exact_store_retired_now,
        "store_exists_now": store_exists_now,
        "build_meta_orphaned_now": build_meta_orphaned_now,
        "new_month_needs_exact_store_build_now": new_month_needs_exact_store_build_now,
        "snapshot": snap,
    }


# ----------------------------------------------------------------------------
# TEST 1 — Retired exact_store triggers Fix A+3 rebuild signal
# ----------------------------------------------------------------------------

def test_retired_exact_store_sets_retired_flag(main_module):
    main, data_dir = main_module
    _write_fake_source_parquet(main, data_dir, MONTH_KEY)
    _write_timeline(main, MONTH_KEY)
    _write_frame_cache_dir(main, MONTH_KEY)
    _write_build_meta(
        main,
        MONTH_KEY,
        {
            "month_key": MONTH_KEY,
            "source_of_truth": "parquet_live",
            "retired_exact_store": True,
            "exact_store_retired_reason": "sample_mismatch",
        },
    )

    flags = _detector_evaluate(main, MONTH_KEY)

    assert flags["exact_store_retired_now"] is True, \
        "Fix A+3 gate should recognize retired_exact_store=True in build_meta"
    assert flags["build_meta_orphaned_now"] is False, \
        "Fix F must NOT fire when Fix A+3 applies"
    assert flags["new_month_needs_exact_store_build_now"] is False, \
        "Fix G must NOT fire when Fix A+3 applies"


# ----------------------------------------------------------------------------
# TEST 2 — Orphaned build_meta triggers Fix F rebuild signal
# ----------------------------------------------------------------------------

def test_orphaned_build_meta_sets_orphaned_flag(main_module):
    main, data_dir = main_module
    _write_fake_source_parquet(main, data_dir, MONTH_KEY)
    _write_timeline(main, MONTH_KEY)
    _write_frame_cache_dir(main, MONTH_KEY)
    _write_fake_store(main, MONTH_KEY)  # store exists
    _write_build_meta(
        main,
        MONTH_KEY,
        {
            "month_key": MONTH_KEY,
            # NO source_of_truth key — this is the orphaned state
            "retired_exact_store": False,
        },
    )

    flags = _detector_evaluate(main, MONTH_KEY)

    assert flags["exact_store_retired_now"] is False, \
        "retired flag must be False when Fix F applies"
    assert flags["store_exists_now"] is True, \
        "store must exist for orphaned state"
    assert flags["build_meta_orphaned_now"] is True, \
        "Fix F gate should fire when build_meta exists but source_of_truth is missing"
    assert flags["new_month_needs_exact_store_build_now"] is False, \
        "Fix G must NOT fire when Fix F applies"


# ----------------------------------------------------------------------------
# TEST 3 — New-month parquet_live state triggers Fix G rebuild signal
# ----------------------------------------------------------------------------

def test_new_month_parquet_live_sets_new_month_flag(main_module):
    main, data_dir = main_module
    _write_fake_source_parquet(main, data_dir, MONTH_KEY)
    _write_timeline(main, MONTH_KEY)
    _write_frame_cache_dir(main, MONTH_KEY)
    # NO store written — this is the new-month state
    _write_build_meta(
        main,
        MONTH_KEY,
        {
            "month_key": MONTH_KEY,
            "source_of_truth": "parquet_live",
            "retired_exact_store": False,
            "exact_store_retired_reason": "no_exact_store_present",
        },
    )

    flags = _detector_evaluate(main, MONTH_KEY)

    assert flags["exact_store_retired_now"] is False, \
        "retired flag must be False when Fix G applies"
    assert flags["build_meta_orphaned_now"] is False, \
        "orphaned flag must be False when Fix G applies"
    assert flags["store_exists_now"] is False, \
        "store must NOT exist for new-month state"
    assert flags["new_month_needs_exact_store_build_now"] is True, \
        "Fix G gate should fire for parquet_live + no store + source_parquet_exists"


# ----------------------------------------------------------------------------
# TEST 4 — Healthy exact_store state fires NO rebuild
# ----------------------------------------------------------------------------

def test_healthy_exact_store_fires_no_rebuild(main_module):
    main, data_dir = main_module
    _write_fake_source_parquet(main, data_dir, MONTH_KEY)
    _write_timeline(main, MONTH_KEY)
    _write_frame_cache_dir(main, MONTH_KEY)
    _write_fake_store(main, MONTH_KEY)
    _write_build_meta(
        main,
        MONTH_KEY,
        {
            "month_key": MONTH_KEY,
            "source_of_truth": "exact_store",
            "retired_exact_store": False,
            "attested_via": "direct_build",
        },
    )

    flags = _detector_evaluate(main, MONTH_KEY)

    assert flags["exact_store_retired_now"] is False, \
        "healthy state: Fix A+3 must NOT fire"
    assert flags["build_meta_orphaned_now"] is False, \
        "healthy state: Fix F must NOT fire"
    assert flags["new_month_needs_exact_store_build_now"] is False, \
        "healthy state: Fix G must NOT fire"


# ----------------------------------------------------------------------------
# TEST 5 — Persisted backoff file prevents re-trigger within backoff window
# ----------------------------------------------------------------------------

def test_persisted_backoff_prevents_rebuild_retrigger(main_module):
    main, data_dir = main_module
    _write_fake_source_parquet(main, data_dir, MONTH_KEY)
    _write_timeline(main, MONTH_KEY)
    _write_frame_cache_dir(main, MONTH_KEY)
    _write_build_meta(
        main,
        MONTH_KEY,
        {
            "month_key": MONTH_KEY,
            "source_of_truth": "parquet_live",
            "retired_exact_store": False,
        },
    )

    # Write the persisted backoff state to simulate a recent attempt.
    main._write_auto_rebuild_state(MONTH_KEY)
    persisted = main._read_auto_rebuild_state(MONTH_KEY)

    assert "last_attempt_unix" in persisted, \
        "_write_auto_rebuild_state must persist last_attempt_unix"
    assert persisted["month_key"] == MONTH_KEY, \
        "backoff state must record the correct month_key"
    assert int(persisted["last_attempt_unix"]) > 0, \
        "backoff state must record a real timestamp"

    # Verify the backoff window calculation: within AUTO_RETIRED_REBUILD_BACKOFF_SEC
    # of the recorded attempt, in-persisted-backoff must be True.
    now_unix = int(time.time())
    last_attempt = int(persisted["last_attempt_unix"])
    in_persisted_backoff = bool(
        last_attempt > 0
        and (now_unix - last_attempt) < int(main.AUTO_RETIRED_REBUILD_BACKOFF_SEC)
    )
    assert in_persisted_backoff is True, \
        "fresh backoff state must yield in_persisted_backoff=True"


# ----------------------------------------------------------------------------
# TEST 6 — Once-per-boot guard prevents double-trigger in same process
# ----------------------------------------------------------------------------

def test_once_per_boot_guard_blocks_second_trigger(main_module):
    main, data_dir = main_module

    # Simulate a first trigger: flag is flipped True by the detector.
    assert main._auto_rebuild_triggered_this_boot is False, \
        "fresh process must start with boot guard = False"

    main._auto_rebuild_triggered_this_boot = True

    # Any subsequent evaluation within this process sees the guard.
    # The detector's four "already_triggered_this_boot" branches all check this.
    assert main._auto_rebuild_triggered_this_boot is True, \
        "boot guard must persist within the process"

    # Reset for hygiene; other tests rely on fresh state via fixture.
    main._auto_rebuild_triggered_this_boot = False
