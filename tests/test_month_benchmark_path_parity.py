"""Both frame paths must apply the month benchmark identically.

Attestation compares the parquet path against the exact-store path over exactly
the rating/bucket/colour keys the benchmark rewrites, and a mismatch RETIRES the
month's exact store. So colour post-processing applied to one path and not the
other does not merely look inconsistent -- it deletes the store the benchmark
itself depends on.
"""
from __future__ import annotations

import main


def _payload(ratings):
    return {"polygons": {"features": [
        {"properties": {
            "LocationID": i + 1,
            "pickups_now": 40.0,
            "earnings_shadow_rating_citywide_v3": r,
            "earnings_shadow_bucket_citywide_v3": "green",
            "earnings_shadow_color_citywide_v3": "#00b050",
        }} for i, r in enumerate(ratings)
    ]}}


def _ratings(p):
    return [f["properties"]["earnings_shadow_rating_citywide_v3"]
            for f in p["polygons"]["features"]]


def test_benchmark_helper_is_a_noop_when_flag_off(monkeypatch):
    monkeypatch.setenv("MONTH_ANCHORED_COLORS", "0")
    p = _payload([90, 70, 50])
    assert _ratings(main._apply_month_benchmark_to_features("2025-07", p)) == [90, 70, 50]


def test_benchmark_helper_is_a_noop_without_a_store(monkeypatch, tmp_path):
    monkeypatch.setenv("MONTH_ANCHORED_COLORS", "1")
    monkeypatch.setattr(main, "_benchmark_reference_store_path", lambda: None)
    monkeypatch.setattr(main, "_month_store_path", lambda mk: tmp_path / "missing.duckdb")
    p = _payload([90, 70, 50])
    assert _ratings(main._apply_month_benchmark_to_features("2025-07", p)) == [90, 70, 50]


def test_both_frame_paths_share_one_benchmark_implementation():
    """Guards the parity structurally: if a future change re-inlines the
    benchmark into only one path, this catches it before attestation does."""
    import inspect
    parquet_src = inspect.getsource(main._build_single_frame_for_month)
    store_src = inspect.getsource(main._build_single_frame_from_exact_store)
    assert "_apply_month_benchmark_to_features" in parquet_src
    assert "_apply_month_benchmark_to_features" in store_src


def test_attestation_compares_the_fields_the_benchmark_rewrites():
    """Documents WHY parity matters: these are the compared keys."""
    normalized = main._normalize_frame_payload_for_compare(_payload([90]))
    keys = set(normalized[1].keys())
    assert "earnings_shadow_rating_citywide_v3" in keys
    assert "earnings_shadow_bucket_citywide_v3" in keys
    assert "earnings_shadow_color_citywide_v3" in keys
