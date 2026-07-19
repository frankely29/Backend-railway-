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
