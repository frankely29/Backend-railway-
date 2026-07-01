"""Citywide "good day / bad day" read for the day-tendency meter.

The map colors answer "where should I be right now." This answers a different,
higher-level question the driver also wants at a glance: "is right now a GOOD or
BAD night to be out compared to a normal night like this?"

It compares the CURRENT frame's citywide demand (total rides in this 20-min bin,
airports excluded) against the TYPICAL citywide demand for the SAME weekday and
time-of-day across the whole active month (the benchmark). So "Saturday 8 PM" is
judged against the month's other Saturday 8 PMs -- not against a flat all-hours
average, which would only measure peak-vs-offpeak. Output is a 0-100 meter
centered at 50 (== a normal night like this), so the existing tendency meter can
render it unchanged.

Pure/stdlib compute helpers so the scoring is unit-testable without the data.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

AIRPORT_ZONE_IDS = (1, 132, 138)

# How far above/below typical maps to the top/bottom of the meter. A night with
# demand == typical reads 50; +/-60% off typical saturates the meter. Chosen so
# ordinary night-to-night swings land in a legible mid-range rather than pinning.
_FULL_SWING_FRACTION = 0.60


def _bin_index_expr(ts_col: str) -> str:
    """SQL for the 20-min bin-of-day index from a local-timestamp text column."""
    return (
        f"CAST((EXTRACT(hour FROM CAST({ts_col} AS TIMESTAMP)) * 60 "
        f"+ EXTRACT(minute FROM CAST({ts_col} AS TIMESTAMP))) / 20 AS INTEGER)"
    )


def _weekday_expr(ts_col: str) -> str:
    # ISODOW: Monday=1 .. Sunday=7. Stable, timezone-agnostic on the local ts text.
    return f"ISODOW(CAST({ts_col} AS TIMESTAMP))"


def compute_day_quality_typicals(con: Any) -> Dict[str, float]:
    """Typical citywide demand per (weekday, bin) over the whole month.

    Sums pickups across non-airport zones within each frame, then averages those
    per-frame citywide totals across every frame that shares the weekday+bin. Keyed
    "<weekday>-<bin>" (weekday ISODOW 1..7). Returns {} on any failure so the caller
    degrades to "unavailable" rather than erroring the meter."""
    airports = ", ".join(str(z) for z in AIRPORT_ZONE_IDS)
    sql = f"""
        WITH per_frame AS (
            SELECT
                {_weekday_expr('exact_bin_local_ts')} AS weekday,
                {_bin_index_expr('exact_bin_local_ts')} AS bin_index,
                SUM(COALESCE(pickups_now, 0)) AS citywide_pickups
            FROM exact_shadow_rows
            WHERE PULocationID NOT IN ({airports})
            GROUP BY exact_bin_local_ts
        )
        SELECT weekday, bin_index, AVG(citywide_pickups) AS typical
        FROM per_frame
        GROUP BY weekday, bin_index
    """
    out: Dict[str, float] = {}
    try:
        for weekday, bin_index, typical in con.execute(sql).fetchall():
            if weekday is None or bin_index is None or typical is None:
                continue
            out[f"{int(weekday)}-{int(bin_index)}"] = float(typical)
    except Exception:
        return {}
    return out


def current_citywide_pickups(con: Any, frame_time_local_iso: str) -> Optional[float]:
    """Total citywide (non-airport) pickups in the requested frame's 20-min bin."""
    airports = ", ".join(str(z) for z in AIRPORT_ZONE_IDS)
    sql = f"""
        SELECT SUM(COALESCE(pickups_now, 0))
        FROM exact_shadow_rows
        WHERE exact_bin_local_ts = ?
          AND PULocationID NOT IN ({airports})
    """
    try:
        row = con.execute(sql, [frame_time_local_iso]).fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    return float(row[0])


def _band_and_label(score: float) -> Tuple[str, str]:
    # Match the day-tendency meter's existing band thresholds/wording.
    if score < 40:
        return "low", "Slow night"
    if score >= 60:
        return "high", "Busy night"
    return "normal", "Normal night"


def day_quality_read(
    current_pickups: Optional[float],
    typical_pickups: Optional[float],
    *,
    weekday_name: Optional[str] = None,
    time_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the meter payload from current vs typical citywide demand.

    score 50 == a normal night like this; >50 busier than typical, <50 slower.
    Returns status='unavailable' (meter hides) when inputs are missing."""
    if (
        current_pickups is None
        or typical_pickups is None
        or typical_pickups <= 0
    ):
        return {"status": "unavailable"}
    rel = (float(current_pickups) - float(typical_pickups)) / float(typical_pickups)
    score = 50.0 + 50.0 * (rel / _FULL_SWING_FRACTION)
    score = max(0.0, min(100.0, score))
    band, label = _band_and_label(score)
    pct_vs_typical = int(round(rel * 100.0))
    sign = "+" if pct_vs_typical >= 0 else ""
    when = " ".join(part for part in (weekday_name, time_label) if part) or "a normal night like this"
    explain = f"{sign}{pct_vs_typical}% vs a typical {when}".strip()
    return {
        "status": "ok",
        "score": int(round(score)),
        "meter_pct": round(score / 100.0, 4),
        "band": band,
        "label": label,
        "current_citywide_pickups": int(round(float(current_pickups))),
        "typical_citywide_pickups": int(round(float(typical_pickups))),
        "pct_vs_typical": pct_vs_typical,
        "explain": explain,
        # Reference identity so the client can recompute the read against the SAME
        # citywide level it renders on the map (keeps meter and map consistent).
        "weekday_name": weekday_name,
        "time_label": time_label,
        "full_swing_fraction": _FULL_SWING_FRACTION,
    }
