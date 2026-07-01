"""The sliced score build must NOT anchor per-slice.

Month-anchoring can't happen inside the sliced build (each 6-hour slice only sees
its own rows), so the slice SQL always emits the composite per-frame rating
regardless of MONTH_ANCHORED_COLORS. The anchoring is a whole-month post-pass
(see test_month_anchor_sql.py). This guards against reintroducing the per-slice
PERCENT_RANK bug.
"""
from __future__ import annotations

import os

import zone_mode_profiles as zmp
from zone_earnings_engine import build_zone_earnings_shadow_sql

_PROFILE = zmp.ZONE_MODE_PROFILES["citywide_v3"]
_COMPOSITE = "99 * earnings_shadow_score_citywide_v3) AS INTEGER) AS earnings_shadow_rating_citywide_v3"


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


def test_slice_sql_is_always_composite_regardless_of_flag():
    on, off = _with_flag("1"), _with_flag("0")
    assert on == off
    assert _COMPOSITE in off
    # the per-slice PERCENT_RANK bug must never come back
    assert "PERCENT_RANK() OVER (ORDER BY LN(1 + COALESCE(pickups_now" not in on
