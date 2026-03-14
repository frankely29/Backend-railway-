from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone
import bisect
import json

import duckdb


WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
def percentile_rank(sorted_values: List[float], value: float) -> float:
    if not sorted_values:
        return 0.5
    if len(sorted_values) == 1:
        return 0.5
    left = bisect.bisect_left(sorted_values, value)
    right = bisect.bisect_right(sorted_values, value)
    avg_rank = (left + right - 1) / 2.0
    return max(0.0, min(1.0, avg_rank / (len(sorted_values) - 1)))


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _band_from_score(score: int) -> str:
    if score <= 34:
        return "low"
    if score <= 64:
        return "normal"
    return "high"


def _label_from_band(band: str) -> str:
    if band == "low":
        return "Low"
    if band == "high":
        return "High"
    return "Normal"


def _bin_label(bin_index: int, bin_minutes: int = 20) -> str:
    minute_of_day = int(bin_index) * int(bin_minutes)
    hour24 = (minute_of_day // 60) % 24
    minute = minute_of_day % 60
    ampm = "AM" if hour24 < 12 else "PM"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    return f"{hour12}:{minute:02d} {ampm}"


def _strength_explain(score_raw: float, weekday_name: str | None, bin_label: str) -> str:
    if score_raw >= 0.65:
        relation = "stronger than most time blocks in this dataset"
    elif score_raw <= 0.34:
        relation = "weaker than most time blocks in this dataset"
    else:
        relation = "near the middle of this dataset"

    if not weekday_name:
        return f"Typical days around {bin_label} are {relation}."
    return f"Typical {weekday_name}s around {bin_label} are {relation}."


def _global_explain(score_raw: float) -> str:
    if score_raw >= 0.65:
        relation = "stronger than most time blocks in this dataset"
    elif score_raw <= 0.34:
        relation = "weaker than most time blocks in this dataset"
    else:
        relation = "near the middle"
    return f"Typical time blocks in this dataset are {relation}."


def _insufficient_payload(first_date: str | None = None, last_date: str | None = None, usable_dates: int = 0) -> Dict[str, Any]:
    return {
        "version": "time_tendency_v1",
        "basis": "historical_expected_timeslot",
        "bin_minutes": 20,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "insufficient_data",
        "weekday_bin": {},
        "bin_only": {},
        "global_baseline": {},
        "dataset": {
            "usable_dates": int(usable_dates),
            "first_date": first_date,
            "last_date": last_date,
        },
        "filters": {
            "dropped_first_last_dates": False,
            "min_daily_pickups_floor": 0,
            "min_daily_pickups_ratio": 0.0,
            "dropped_low_sample_dates": 0,
        },
    }


def build_day_tendency_model(
    parquet_files: List[Path],
    out_dir: Path,
    bin_minutes: int = 20,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / "model.json"

    if not parquet_files:
        payload = _insufficient_payload()
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "weekday_bin_cohorts": 0,
            "bin_only_cohorts": 0,
        }

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")

    sql = f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        pickup_datetime,
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL AND pickup_datetime IS NOT NULL
    ),
    t AS (
      SELECT
        PULocationID,
        pickup_datetime,
        CAST(pickup_datetime AS DATE) AS nyc_date,
        CAST(EXTRACT('month' FROM pickup_datetime) AS INTEGER) AS month_i,
        CASE
          WHEN CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) = 0 THEN 6
          ELSE CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) - 1
        END AS dow_m,
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay
      FROM base
    ),
    daily AS (
      SELECT
        nyc_date,
        month_i,
        dow_m,
        COUNT(*) AS daily_pickups,
        AVG(driver_pay) AS avg_driver_pay,
        COUNT(DISTINCT PULocationID) AS active_zones
      FROM t
      GROUP BY 1,2,3
    ),
    binned AS (
      SELECT
        nyc_date,
        CAST(FLOOR((hour_i * 60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
        COUNT(*) AS bin_pickups
      FROM t
      GROUP BY 1,2
    ),
    peaks AS (
      SELECT
        nyc_date,
        MAX(bin_pickups) AS peak_20m_pickups
      FROM binned
      GROUP BY 1
    )
    SELECT
      nyc_date,
      month_i,
      dow_m,
      CAST(FLOOR((hour_i * 60 + minute_i) / {int(bin_minutes)}) AS INTEGER) AS bin_index,
      COUNT(*) AS pickups_bin,
      AVG(driver_pay) AS avg_driver_pay_bin,
      COUNT(DISTINCT PULocationID) AS active_zones_bin
    FROM t
    GROUP BY 1,2,3,4
    ORDER BY nyc_date, bin_index
    """

    rows = con.execute(sql).fetchall()
    con.close()

    if len(rows) < 1:
        payload = _insufficient_payload(usable_dates=0)
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "weekday_bin_cohorts": 0,
            "bin_only_cohorts": 0,
        }

    dates = [r[0] for r in rows]
    first_date = str(min(dates)) if dates else None
    last_date = str(max(dates)) if dates else None

    usable_rows = rows
    usable_dates = {r[0] for r in usable_rows}
    dropped_low_sample_dates = 0

    def build_cohorts(source_rows: List[Any], key_fn, cohort_type: str) -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Any]] = {}
        for r in source_rows:
            k = key_fn(r)
            grouped.setdefault(k, []).append(r)

        cohorts: Dict[str, Dict[str, Any]] = {}
        for key, g in grouped.items():
            dow_m = int(g[0][2])
            bin_idx = int(g[0][3])
            pickups_bin_median = _median([float(x[4]) for x in g])
            avg_driver_pay_bin_median = _median([float(x[5]) if x[5] is not None else 0.0 for x in g])
            active_zones_bin_median = _median([float(x[6]) for x in g])

            item: Dict[str, Any] = {
                "bin_index": bin_idx,
                "bin_label": _bin_label(bin_idx, int(bin_minutes)),
                "sample_bins": len(g),
                "pickups_bin_median": int(round(pickups_bin_median)),
                "avg_driver_pay_bin_median": round(avg_driver_pay_bin_median, 2),
                "active_zones_bin_median": int(round(active_zones_bin_median)),
                "cohort_type": cohort_type,
            }
            if cohort_type == "weekday_bin":
                item.update({"weekday": dow_m, "weekday_name": WEEKDAY_NAMES[dow_m]})
            cohorts[key] = item
        return cohorts

    weekday_bin = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{int(r[2])}-{int(r[3])}",
        cohort_type="weekday_bin",
    )
    bin_only = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{int(r[3])}",
        cohort_type="bin_only",
    )

    def score_cohorts(cohorts: Dict[str, Dict[str, Any]], explain_weekday: bool) -> None:
        pickup_values = sorted([float(v["pickups_bin_median"]) for v in cohorts.values()])
        pay_values = sorted([float(v["avg_driver_pay_bin_median"]) for v in cohorts.values()])
        breadth_values = sorted([float(v["active_zones_bin_median"]) for v in cohorts.values()])

        for v in cohorts.values():
            pickup_strength = percentile_rank(pickup_values, float(v["pickups_bin_median"]))
            pay_strength = percentile_rank(pay_values, float(v["avg_driver_pay_bin_median"]))
            breadth_strength = percentile_rank(breadth_values, float(v["active_zones_bin_median"]))

            score_raw = 0.70 * pickup_strength + 0.15 * pay_strength + 0.15 * breadth_strength
            score = int(round(100 * score_raw))
            score = max(0, min(100, score))
            band = _band_from_score(score)

            v["pickup_strength"] = round(pickup_strength, 4)
            v["pay_strength"] = round(pay_strength, 4)
            v["breadth_strength"] = round(breadth_strength, 4)
            v["score_raw"] = round(score_raw, 4)
            v["score"] = score
            v["band"] = band
            v["label"] = _label_from_band(band)
            v["confidence"] = round(min(1.0, float(v["sample_bins"]) / 12.0), 2)
            if explain_weekday:
                v["explain"] = _strength_explain(
                    score_raw=score_raw,
                    weekday_name=str(v["weekday_name"]) if v.get("weekday_name") else None,
                    bin_label=str(v["bin_label"]),
                )
            else:
                v["explain"] = _strength_explain(
                    score_raw=score_raw,
                    weekday_name=None,
                    bin_label=str(v["bin_label"]),
                )

    score_cohorts(weekday_bin, explain_weekday=True)
    score_cohorts(bin_only, explain_weekday=False)

    bin_pickup_values = sorted([float(v["pickups_bin_median"]) for v in bin_only.values()])
    bin_pay_values = sorted([float(v["avg_driver_pay_bin_median"]) for v in bin_only.values()])
    bin_breadth_values = sorted([float(v["active_zones_bin_median"]) for v in bin_only.values()])

    global_pickups_median = _median([float(r[4]) for r in usable_rows])
    global_pay_median = _median([float(r[5]) if r[5] is not None else 0.0 for r in usable_rows])
    global_breadth_median = _median([float(r[6]) for r in usable_rows])

    global_pickup_strength = percentile_rank(bin_pickup_values, global_pickups_median) if bin_pickup_values else 0.5
    global_pay_strength = percentile_rank(bin_pay_values, global_pay_median) if bin_pay_values else 0.5
    global_breadth_strength = percentile_rank(bin_breadth_values, global_breadth_median) if bin_breadth_values else 0.5
    global_score_raw = 0.70 * global_pickup_strength + 0.15 * global_pay_strength + 0.15 * global_breadth_strength
    global_score = int(round(100 * global_score_raw))
    global_score = max(0, min(100, global_score))
    global_band = _band_from_score(global_score)

    global_baseline = {
        "sample_bins": len(usable_rows),
        "pickups_bin_median": int(round(global_pickups_median)),
        "avg_driver_pay_bin_median": round(global_pay_median, 2),
        "active_zones_bin_median": int(round(global_breadth_median)),
        "pickup_strength": round(global_pickup_strength, 4),
        "pay_strength": round(global_pay_strength, 4),
        "breadth_strength": round(global_breadth_strength, 4),
        "score_raw": round(global_score_raw, 4),
        "score": global_score,
        "band": global_band,
        "label": _label_from_band(global_band),
        "confidence": round(min(1.0, len(usable_rows) / 12.0), 2),
        "cohort_type": "global_baseline",
        "explain": _global_explain(global_score_raw),
    }

    payload = {
        "version": "time_tendency_v1",
        "basis": "historical_expected_timeslot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bin_minutes": int(bin_minutes),
        "filters": {
            "dropped_first_last_dates": False,
            "min_daily_pickups_floor": 0,
            "min_daily_pickups_ratio": 0.0,
            "dropped_low_sample_dates": dropped_low_sample_dates,
        },
        "weekday_bin": weekday_bin,
        "bin_only": bin_only,
        "global_baseline": global_baseline,
        "dataset": {
            "usable_dates": len(usable_dates),
            "first_date": first_date,
            "last_date": last_date,
        },
    }

    model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "model_path": str(model_path),
        "usable_dates": len(usable_dates),
        "weekday_bin_cohorts": len(weekday_bin),
        "bin_only_cohorts": len(bin_only),
    }
