"""Build-time sidecar for the month demand breakpoints.

The parquet serve path can't reliably reopen the live DuckDB store at serve time
(lock / version fragility), which left month-anchored colors silently disabled.
The fix persists the breakpoints as a JSON sidecar next to the store at build
time; serve time reads that cheap file and self-heals (recompute + rewrite) if it
is missing. These tests lock in that resolution order.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import build_hotspot as bh


def _make_store(tmp_path: Path) -> Path:
    store = tmp_path / "exact_shadow.duckdb"
    con = duckdb.connect(str(store))
    con.execute("CREATE TABLE exact_shadow_rows(PULocationID INTEGER, pickups_now INTEGER)")
    rows = []
    for zone_id in range(2, 60):
        for pickups in range(0, zone_id):
            rows.append((zone_id, pickups))
    # Airports carry huge demand but must be excluded from the benchmark.
    rows += [(1, 999), (132, 999), (138, 999)]
    con.executemany("INSERT INTO exact_shadow_rows VALUES (?, ?)", rows)
    con.close()
    return store


@pytest.fixture(autouse=True)
def _clear_cache():
    bh._MONTH_DEMAND_BP_CACHE.clear()
    bh._MONTH_DEMAND_BP_LAST_ERROR.clear()
    yield
    bh._MONTH_DEMAND_BP_CACHE.clear()


def test_build_time_writes_sidecar(tmp_path: Path):
    store = _make_store(tmp_path)
    con = duckdb.connect(str(store), read_only=True)
    try:
        bps = bh.compute_and_persist_demand_breakpoints(con, store)
    finally:
        con.close()
    assert len(bps) == 101
    assert bps == sorted(bps)
    assert bh._demand_breakpoints_sidecar_path(store).exists()


def test_serve_reads_sidecar_without_opening_store(tmp_path: Path):
    store = _make_store(tmp_path)
    con = duckdb.connect(str(store), read_only=True)
    try:
        built = bh.compute_and_persist_demand_breakpoints(con, store)
    finally:
        con.close()
    bh._MONTH_DEMAND_BP_CACHE.clear()

    # Corrupt the DuckDB file so any reopen would fail; the sidecar must still work.
    store.write_bytes(b"not a duckdb file")
    served = bh.month_demand_breakpoints_for_store(store)
    assert served == built


def test_serve_self_heals_when_sidecar_missing(tmp_path: Path):
    store = _make_store(tmp_path)
    sidecar = bh._demand_breakpoints_sidecar_path(store)
    assert not sidecar.exists()

    served = bh.month_demand_breakpoints_for_store(store)
    assert len(served) == 101
    # Self-heal must have persisted the sidecar for subsequent serves.
    assert sidecar.exists()


def test_missing_store_reports_error_and_empty(tmp_path: Path):
    missing = tmp_path / "nope" / "exact_shadow.duckdb"
    assert bh.month_demand_breakpoints_for_store(missing) == []
    assert bh._MONTH_DEMAND_BP_LAST_ERROR.get("open") == "store_missing_or_empty"
