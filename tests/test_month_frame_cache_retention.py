"""Retention pruning of old months' SERVED frame caches.

The /data volume grew without bound because every calendar month's served
frame cache (frame_*.json, ~1-2 GB/month) was kept forever. The prune keeps
the active month warm and reclaims older months' caches -- while NEVER touching
source parquet files, exact stores, or any user data.
"""
import shutil
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import main


def _current_month_key() -> str:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m")


def _seed_frame_cache(month_key: str, count: int = 2):
    cache_dir = main._month_frame_cache_dir(month_key)
    cache_dir.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        (cache_dir / f"frame_{i:05d}_t.json").write_text("{}", encoding="utf-8")
    return cache_dir


def test_prune_keeps_active_month_and_protects_sources(monkeypatch):
    current = _current_month_key()
    old_a, old_b = "2019-01", "2019-02"
    months = [old_a, old_b, current]
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {"available_month_keys": list(months)})

    parquet = main.DATA_DIR / "test_retention_keep.parquet"
    try:
        cache_dirs = {mk: _seed_frame_cache(mk) for mk in months}
        # Protected artifacts that must survive the prune.
        store_old = main._month_store_path(old_a)
        store_old.parent.mkdir(parents=True, exist_ok=True)
        store_old.write_text("DUCKDB", encoding="utf-8")
        parquet.write_text("PARQUET", encoding="utf-8")

        result = main._prune_inactive_month_frame_caches()

        # Old months' served frame caches reclaimed.
        assert list(cache_dirs[old_a].glob("frame_*.json")) == []
        assert list(cache_dirs[old_b].glob("frame_*.json")) == []
        # Active month stays warm.
        assert list(cache_dirs[current].glob("frame_*.json"))
        # Derived store + source parquet are NEVER touched.
        assert store_old.exists()
        assert parquet.exists()
        assert set(result["pruned_months"]) == {old_a, old_b}
        assert result["removed_frame_count"] == 4
        assert result["bytes_freed_estimate"] > 0
    finally:
        parquet.unlink(missing_ok=True)
        for mk in months:
            shutil.rmtree(main._month_dir(mk), ignore_errors=True)


def test_prune_noop_without_manifest(monkeypatch):
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {})
    result = main._prune_inactive_month_frame_caches()
    assert result == {"pruned_months": [], "removed_frame_count": 0, "bytes_freed_estimate": 0}


def _seed_month_dir(month_key: str):
    """A published month dir: store + a frame cache file."""
    d = main._month_dir(month_key)
    d.mkdir(parents=True, exist_ok=True)
    (d / "exact_shadow.duckdb").write_text("X" * 2048, encoding="utf-8")
    fc = main._month_frame_cache_dir(month_key)
    fc.mkdir(parents=True, exist_ok=True)
    (fc / "frame_00000_t.json").write_text("{}", encoding="utf-8")
    return d


def test_reclaim_orphan_month_dirs_removes_unmanifested(monkeypatch):
    current = _current_month_key()  # in manifest + active -> keep
    orphan_a, orphan_b = "2017-03", "2017-04"  # on disk, NOT in manifest -> reclaim
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {"available_month_keys": [current]})
    months = [current, orphan_a, orphan_b]
    try:
        for mk in months:
            _seed_month_dir(mk)
        result = main._reclaim_orphan_month_dirs()
        assert not main._month_dir(orphan_a).exists()
        assert not main._month_dir(orphan_b).exists()
        assert main._month_dir(current).exists()  # manifest/active month protected
        assert set(result["removed_month_dirs"]) >= {orphan_a, orphan_b}
        assert result["bytes_freed_estimate"] > 0
    finally:
        for mk in months:
            shutil.rmtree(main._month_dir(mk), ignore_errors=True)


def test_reclaim_orphan_month_dirs_noop_without_manifest(monkeypatch):
    # Empty manifest -> never delete (don't risk removing the only/served month).
    orphan = "2017-05"
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {})
    try:
        _seed_month_dir(orphan)
        result = main._reclaim_orphan_month_dirs()
        assert main._month_dir(orphan).exists()
        assert result["removed_month_dirs"] == []
    finally:
        shutil.rmtree(main._month_dir(orphan), ignore_errors=True)


def test_prune_keeps_only_active_when_it_is_newest(monkeypatch):
    # keep_recent=1 means only the newest (== active) month survives.
    current = _current_month_key()
    older = "2018-07"
    months = [older, current]
    monkeypatch.setattr(main, "_load_month_manifest", lambda: {"available_month_keys": list(months)})
    try:
        d_old = _seed_frame_cache(older, count=3)
        d_cur = _seed_frame_cache(current, count=3)
        result = main._prune_inactive_month_frame_caches(keep_recent=1)
        assert list(d_old.glob("frame_*.json")) == []
        assert len(list(d_cur.glob("frame_*.json"))) == 3
        assert result["pruned_months"] == [older]
    finally:
        for mk in months:
            shutil.rmtree(main._month_dir(mk), ignore_errors=True)
