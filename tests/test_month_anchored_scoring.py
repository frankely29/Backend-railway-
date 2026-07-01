"""Build-time flag for month-anchored colors (Option A).

Default OFF must be byte-identical to the current per-frame rating. When
MONTH_ANCHORED_COLORS=1, the citywide v3 rating becomes the whole-month percentile
of raw demand, so colors mean the same busy-ness every day. This locks the flag
plumbing; the live cutover is a coordinated month rebuild.
"""
from __future__ import annotations

import os

import zone_mode_profiles as zmp
from zone_earnings_engine import build_zone_earnings_shadow_sql

_PROFILE = zmp.ZONE_MODE_PROFILES["citywide_v3"]
_COMPOSITE = "99 * earnings_shadow_score_citywide_v3) AS INTEGER) AS earnings_shadow_rating_citywide_v3"
_MONTH = "PERCENT_RANK() OVER (ORDER BY LN(1 + COALESCE(pickups_now, 0)))) AS INTEGER) AS earnings_shadow_rating_citywide_v3"
_BUCKET = "WHEN earnings_shadow_rating_citywide_v3 >= 83 THEN 'green'"


def _gen():
    return build_zone_earnings_shadow_sql(
        ["/tmp/x.parquet"], bin_minutes=20, min_trips_per_window=5, profile=_PROFILE,
    )


def _with_flag(value):
    prev = os.environ.get("MONTH_ANCHORED_COLORS")
    if value is None:
        os.environ.pop("MONTH_ANCHORED_COLORS", None)
    else:
        os.environ["MONTH_ANCHORED_COLORS"] = value
    try:
        return _gen()
    finally:
        if prev is None:
            os.environ.pop("MONTH_ANCHORED_COLORS", None)
        else:
            os.environ["MONTH_ANCHORED_COLORS"] = prev


def test_flag_off_keeps_the_current_per_frame_rating():
    sql = _with_flag(None)
    assert _COMPOSITE in sql
    assert "PERCENT_RANK() OVER (ORDER BY LN(1 + COALESCE(pickups_now" not in sql
    assert _BUCKET in sql  # bucket/color thresholds unchanged


def test_flag_off_is_identical_to_default():
    assert _with_flag(None) == _with_flag("0")


def test_flag_on_switches_citywide_rating_to_month_wide_demand_percentile():
    sql = _with_flag("1")
    assert _MONTH in sql
    assert _COMPOSITE not in sql
    # composite score column is still produced (available to blend/refine later)
    assert "earnings_shadow_score_citywide_v3," in sql
    # colors and guidance both read this same alias, so they stay consistent
    assert _BUCKET in sql


def test_flag_on_only_changes_the_rating_length_by_the_expression_swap():
    off, on = _with_flag("0"), _with_flag("1")
    # Only the one rating expression differs — everything else is untouched.
    assert abs(len(on) - len(off)) < 80
