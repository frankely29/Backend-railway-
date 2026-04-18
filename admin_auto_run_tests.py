"""
Auto-run startup test harness.

On Railway startup, after other auto-prepare work is complete, the backend
automatically invokes the 8 most critical admin diagnostic tests and caches
their responses in module-level memory. The admin portal displays these
results so the operator can see at a glance whether the current deploy is
healthy without manually clicking "Run All Tests".

Design choices (explicit):
- Module-level memory only. Results reset on each Railway restart.
  This is intentional — the results represent "state of this deploy."
- Tests run sequentially on the startup thread. Total runtime for 8 tests
  is typically well under 1 second because each test is a local query.
- Each test is wrapped in try/except so one failing test does NOT abort
  the rest or block startup.
- Results are NOT persisted to disk or Postgres. If you want persistence
  later, add it here; no other module depends on this being in-memory.
"""
from __future__ import annotations

import time
import traceback
from typing import Any, Callable, Dict, List

# Imported lazily to avoid circular imports during module load.
_TEST_SPECS_CACHE: List[Dict[str, Any]] | None = None

# Module-level cache. Reset on each process start. Reader: get_last_results().
_results: Dict[str, Any] = {
    "ran_at_unix": 0,
    "ran_at_utc": "",
    "backend_build_id": None,
    "items": [],
    "pass_count": 0,
    "fail_count": 0,
    "total_count": 0,
}


def _test_specs() -> List[Dict[str, Any]]:
    """
    Lazy-initialized list of test specs. Each spec is:
        {"key": str, "label": str, "callable": Callable[[], Dict[str, Any]]}

    Callables must return AdminDiagnosticResponse-shaped dicts:
        {"ok": bool, "test_name": str, "checked_at": str, "summary": str, "details": dict}
    """
    global _TEST_SPECS_CACHE
    if _TEST_SPECS_CACHE is not None:
        return _TEST_SPECS_CACHE

    from admin_test_service import (
        test_backend_status,
        test_timeline,
        test_frame_current,
        test_score_manifest,
        test_score_frame_integrity,
        test_generated_artifact_sync,
        test_presence_summary,
        test_trips_summary,
    )

    _TEST_SPECS_CACHE = [
        {"key": "backend-status", "label": "Backend Status", "callable": test_backend_status},
        {"key": "timeline", "label": "Timeline Ready", "callable": test_timeline},
        {"key": "frame-current", "label": "Current Frame", "callable": test_frame_current},
        {"key": "score-manifest", "label": "Score Manifest", "callable": test_score_manifest},
        {"key": "score-frame-integrity", "label": "Score Frame Integrity", "callable": test_score_frame_integrity},
        {"key": "generated-artifact-sync", "label": "Generated Artifact Sync", "callable": test_generated_artifact_sync},
        {"key": "presence-summary", "label": "Presence Summary", "callable": test_presence_summary},
        {"key": "trips-summary", "label": "Trips Summary", "callable": test_trips_summary},
    ]
    return _TEST_SPECS_CACHE


def _current_backend_build_id() -> str | None:
    """
    Best-effort read of backend_build_id from main. Safe if main isn't fully
    loaded yet (returns None in that case).
    """
    try:
        from main import _artifact_freshness_snapshot, _backend_identity_snapshot
        identity = _backend_identity_snapshot(_artifact_freshness_snapshot())
        return identity.get("backend_build_id")
    except Exception:
        return None


def _invoke_one(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Invoke one test callable and return a normalized result envelope.
    Never raises — failures become fail results with error detail.
    """
    key = str(spec.get("key") or "").strip()
    label = str(spec.get("label") or key).strip()
    fn: Callable[[], Dict[str, Any]] = spec.get("callable")
    started = time.time()
    try:
        response = fn()
        if not isinstance(response, dict):
            raise TypeError(f"Test {key} returned non-dict: {type(response).__name__}")
        elapsed_ms = int((time.time() - started) * 1000)
        return {
            "key": key,
            "label": label,
            "ok": bool(response.get("ok")),
            "test_name": str(response.get("test_name") or key),
            "checked_at": str(response.get("checked_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            "summary": str(response.get("summary") or ""),
            "details": response.get("details") or {},
            "elapsed_ms": elapsed_ms,
            "error": None,
        }
    except Exception as exc:
        elapsed_ms = int((time.time() - started) * 1000)
        return {
            "key": key,
            "label": label,
            "ok": False,
            "test_name": key,
            "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "summary": f"Auto-run of {label} raised an exception.",
            "details": {"error": str(exc), "traceback": traceback.format_exc()},
            "elapsed_ms": elapsed_ms,
            "error": str(exc),
        }


def run_startup_tests() -> Dict[str, Any]:
    """
    Sequentially invoke the 8 auto-run tests, cache results in module memory,
    and return the cache snapshot. Safe to call multiple times; each call
    overwrites the previous results.

    Called from main.py's startup() handler after auto-prepare is done.
    """
    global _results
    specs = _test_specs()
    items: List[Dict[str, Any]] = []
    pass_count = 0
    fail_count = 0
    for spec in specs:
        result = _invoke_one(spec)
        items.append(result)
        if result["ok"]:
            pass_count += 1
        else:
            fail_count += 1

    snapshot = {
        "ran_at_unix": int(time.time()),
        "ran_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "backend_build_id": _current_backend_build_id(),
        "items": items,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "total_count": len(items),
    }
    _results = snapshot
    print(
        f"admin_auto_run_tests_completed "
        f"total={snapshot['total_count']} pass={pass_count} fail={fail_count} "
        f"build_id={snapshot['backend_build_id']}"
    )
    return snapshot


def get_last_results() -> Dict[str, Any]:
    """
    Returns the most recent auto-run results. If run_startup_tests has never
    been called in this process, returns the zero-state snapshot which the
    frontend can distinguish via total_count=0 and ran_at_unix=0.
    """
    return dict(_results)
