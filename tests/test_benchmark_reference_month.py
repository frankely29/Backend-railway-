"""Cross-month benchmark reference.

Colors/Tendency anchor to a strong-normal reference month (default October) so a
slow active month reads accurately dim, not inflated. The reference degrades to
the active month until its store is built (no regression), and its store must be
protected from the cleanup that retires non-active months.
"""
from __future__ import annotations

import importlib
import os

import main


def test_reference_month_defaults_to_october(monkeypatch):
    monkeypatch.delenv("BENCHMARK_REFERENCE_MONTH", raising=False)
    assert main._benchmark_reference_month_key() == "2025-10"


def test_reference_month_env_override(monkeypatch):
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "2025-05")
    assert main._benchmark_reference_month_key() == "2025-05"


def test_reference_store_path_none_when_missing(monkeypatch, tmp_path):
    # A month whose store does not exist -> None, so callers fall back to active.
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "1999-01")
    assert main._benchmark_reference_store_path() is None


def test_reference_month_store_is_protected_from_retirement(monkeypatch):
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "2025-10")
    result = main._retire_obsolete_exact_store("2025-10", reason="unit_test")
    assert result["removed_store"] is False
    assert result["reason"] == "protected_benchmark_reference_month"


def test_non_reference_month_not_protected(monkeypatch):
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "2025-10")
    # A different month is NOT short-circuited by the protection guard (it proceeds
    # to normal retirement logic; store simply doesn't exist here so nothing removed).
    result = main._retire_obsolete_exact_store("1999-02", reason="unit_test")
    assert result["reason"] != "protected_benchmark_reference_month"


def test_reclaim_orphan_dirs_keeps_reference_month(monkeypatch, tmp_path):
    # Reproduces the bug: the startup orphan-reclaim deleted the reference month
    # (not in the manifest, not active) on deploy. It must now keep it.
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "2025-10")
    months_dir = tmp_path / "months"
    months_dir.mkdir()
    for mk in ("2025-07", "2025-10", "2025-03"):
        (months_dir / mk).mkdir()
        (months_dir / mk / "exact_shadow.duckdb").write_bytes(b"x" * 16)
    monkeypatch.setattr(main, "EXACT_HISTORY_MONTHS_DIR", months_dir)
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {"available_month_keys": ["2025-07"]})
    monkeypatch.setattr(main, "resolve_active_month_key", lambda *a, **k: "2025-07")

    res = main._reclaim_orphan_month_dirs()

    assert (months_dir / "2025-10").exists(), "reference month must be protected from reclaim"
    assert (months_dir / "2025-07").exists(), "active/manifest month kept"
    assert not (months_dir / "2025-03").exists(), "true orphan should be removed"
    assert "2025-03" in res["removed_month_dirs"]
    assert "2025-10" not in res["removed_month_dirs"]


def _patch_common(monkeypatch, active="2025-07", sources=("2025-07", "2025-10")):
    monkeypatch.setenv("BENCHMARK_REFERENCE_MONTH", "2025-10")
    monkeypatch.setattr(main, "_available_source_month_keys", lambda: list(sources))
    monkeypatch.setattr(main, "resolve_active_month_key", lambda *a, **k: active)
    monkeypatch.setattr(main, "_get_state", lambda: {"state": "idle"})
    monkeypatch.setattr(main, "_read_auto_rebuild_state", lambda mk: {})
    monkeypatch.setattr(main, "_write_auto_rebuild_state", lambda mk: None)


def test_ensure_reference_skips_when_reference_is_active(monkeypatch):
    _patch_common(monkeypatch, active="2025-10")
    assert main._ensure_benchmark_reference_month_ready()["action"] == "is_active_month"


def test_ensure_reference_no_parquet(monkeypatch):
    _patch_common(monkeypatch, active="2025-07", sources=("2025-07",))
    assert main._ensure_benchmark_reference_month_ready()["action"] == "no_source_parquet"


def test_ensure_reference_builds_when_missing(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    monkeypatch.setattr(main, "_month_store_path", lambda mk: tmp_path / "nope" / "exact_shadow.duckdb")
    called = {}
    monkeypatch.setattr(main, "start_generate", lambda *a, **k: called.setdefault("mk", k.get("month_key")))
    res = main._ensure_benchmark_reference_month_ready()
    assert res["action"] == "build_started" and res["reason"] == "missing"
    assert called["mk"] == "2025-10"


def test_ensure_reference_fresh_when_hash_matches(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    store = tmp_path / "exact_shadow.duckdb"; store.write_bytes(b"x" * 100)
    monkeypatch.setattr(main, "_month_store_path", lambda mk: store)
    monkeypatch.setattr(main, "load_month_build_meta", lambda d, mk: {"code_dependency_hash": "SAME"})
    monkeypatch.setattr(main, "start_generate", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not build")))
    assert main._ensure_benchmark_reference_month_ready()["action"] == "fresh"


def test_ensure_reference_rebuilds_when_hash_stale(monkeypatch, tmp_path):
    _patch_common(monkeypatch)
    store = tmp_path / "exact_shadow.duckdb"; store.write_bytes(b"x" * 100)
    monkeypatch.setattr(main, "_month_store_path", lambda mk: store)
    monkeypatch.setattr(main, "load_month_build_meta",
                        lambda d, mk: {"code_dependency_hash": "REF" if mk == "2025-10" else "ACTIVE"})
    called = {}
    monkeypatch.setattr(main, "start_generate", lambda *a, **k: called.setdefault("mk", k.get("month_key")))
    res = main._ensure_benchmark_reference_month_ready()
    assert res["action"] == "build_started" and res["reason"] == "stale_rating_logic"
