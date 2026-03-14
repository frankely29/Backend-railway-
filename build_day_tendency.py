from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone
import bisect
import json

import duckdb


WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


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


def _strength_explain(score_raw: float, weekday_name: str, month: int | None) -> str:
    if score_raw >= 0.65:
        relation = "stronger than most days in this dataset"
    elif score_raw <= 0.34:
        relation = "weaker than most days in this dataset"
    else:
        relation = "near the middle of this dataset"

    if month is None:
        return f"Typical {weekday_name}s are {relation}."
    month_name = MONTH_NAMES[month - 1]
    return f"Typical {weekday_name}s in {month_name} are {relation}."


def _insufficient_payload(first_date: str | None = None, last_date: str | None = None, usable_dates: int = 0) -> Dict[str, Any]:
    return {
        "version": "day_tendency_v1",
        "basis": "historical_expected_day",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "insufficient_data",
        "dataset": {
            "usable_dates": int(usable_dates),
            "first_date": first_date,
            "last_date": last_date,
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
            "month_weekday_cohorts": 0,
            "weekday_only_cohorts": 0,
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
      d.nyc_date,
      d.month_i,
      d.dow_m,
      d.daily_pickups,
      d.avg_driver_pay,
      d.active_zones,
      COALESCE(p.peak_20m_pickups, 0) AS peak_20m_pickups
    FROM daily d
    LEFT JOIN peaks p USING (nyc_date)
    ORDER BY d.nyc_date
    """

    rows = con.execute(sql).fetchall()
    con.close()

    if len(rows) < 3:
        payload = _insufficient_payload(usable_dates=0)
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "month_weekday_cohorts": 0,
            "weekday_only_cohorts": 0,
        }

    dates = [r[0] for r in rows]
    first_date = str(min(dates)) if dates else None
    last_date = str(max(dates)) if dates else None
    edge_excluded_dates = set()
    if dates:
        edge_excluded_dates.add(min(dates))
        edge_excluded_dates.add(max(dates))

    filtered = [r for r in rows if r[0] not in edge_excluded_dates]
    if not filtered:
        payload = _insufficient_payload(first_date=first_date, last_date=last_date, usable_dates=0)
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "month_weekday_cohorts": 0,
            "weekday_only_cohorts": 0,
        }

    global_median_daily_pickups = _median([float(r[3]) for r in filtered])
    min_daily_pickups = max(200.0, 0.20 * global_median_daily_pickups)

    usable_rows = [r for r in filtered if float(r[3]) >= min_daily_pickups]
    dropped_low_sample_dates = len(filtered) - len(usable_rows)

    if len(usable_rows) < 7:
        payload = _insufficient_payload(first_date=first_date, last_date=last_date, usable_dates=len(usable_rows))
        payload["filters"] = {
            "dropped_first_last_dates": True,
            "min_daily_pickups_floor": 200,
            "min_daily_pickups_ratio": 0.2,
            "dropped_low_sample_dates": dropped_low_sample_dates,
        }
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": len(usable_rows),
            "month_weekday_cohorts": 0,
            "weekday_only_cohorts": 0,
        }

    def build_cohorts(source_rows: List[Any], key_fn, with_month: bool) -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Any]] = {}
        for r in source_rows:
            k = key_fn(r)
            grouped.setdefault(k, []).append(r)

        cohorts: Dict[str, Dict[str, Any]] = {}
        for key, g in grouped.items():
            month_i = int(g[0][1])
            dow_m = int(g[0][2])
            daily_pickups_median = _median([float(x[3]) for x in g])
            avg_driver_pay_median = _median([float(x[4]) if x[4] is not None else 0.0 for x in g])
            active_zones_median = _median([float(x[5]) for x in g])
            peak_20m_pickups_median = _median([float(x[6]) for x in g])

            item: Dict[str, Any] = {
                "sample_days": len(g),
                "daily_pickups_median": int(round(daily_pickups_median)),
                "avg_driver_pay_median": round(avg_driver_pay_median, 2),
                "active_zones_median": int(round(active_zones_median)),
                "peak_20m_pickups_median": int(round(peak_20m_pickups_median)),
            }
            if with_month:
                item.update({"month": month_i, "weekday": dow_m, "weekday_name": WEEKDAY_NAMES[dow_m]})
            else:
                item.update({"weekday": dow_m, "weekday_name": WEEKDAY_NAMES[dow_m]})
            cohorts[key] = item
        return cohorts

    month_weekday = build_cohorts(usable_rows, key_fn=lambda r: f"{int(r[1])}-{int(r[2])}", with_month=True)
    weekday_only = build_cohorts(usable_rows, key_fn=lambda r: f"{int(r[2])}", with_month=False)

    def score_cohorts(cohorts: Dict[str, Dict[str, Any]], with_month: bool) -> None:
        pickup_values = sorted([float(v["daily_pickups_median"]) for v in cohorts.values()])
        pay_values = sorted([float(v["avg_driver_pay_median"]) for v in cohorts.values()])
        breadth_values = sorted([float(v["active_zones_median"]) for v in cohorts.values()])
        peak_values = sorted([float(v["peak_20m_pickups_median"]) for v in cohorts.values()])

        for v in cohorts.values():
            pickup_strength = percentile_rank(pickup_values, float(v["daily_pickups_median"]))
            pay_strength = percentile_rank(pay_values, float(v["avg_driver_pay_median"]))
            breadth_strength = percentile_rank(breadth_values, float(v["active_zones_median"]))
            peak_strength = percentile_rank(peak_values, float(v["peak_20m_pickups_median"]))

            score_raw = 0.70 * pickup_strength + 0.10 * pay_strength + 0.10 * breadth_strength + 0.10 * peak_strength
            score = int(round(100 * score_raw))
            score = max(0, min(100, score))
            band = _band_from_score(score)

            v["pickup_strength"] = round(pickup_strength, 4)
            v["pay_strength"] = round(pay_strength, 4)
            v["breadth_strength"] = round(breadth_strength, 4)
            v["peak_strength"] = round(peak_strength, 4)
            v["score_raw"] = round(score_raw, 4)
            v["score"] = score
            v["band"] = band
            v["label"] = _label_from_band(band)
            v["confidence"] = round(min(1.0, float(v["sample_days"]) / 12.0), 2)
            v["cohort_type"] = "same_month_same_weekday" if with_month else "weekday_only"
            v["explain"] = _strength_explain(
                score_raw=score_raw,
                weekday_name=str(v["weekday_name"]),
                month=int(v["month"]) if with_month else None,
            )

    score_cohorts(month_weekday, with_month=True)
    score_cohorts(weekday_only, with_month=False)

    payload = {
        "version": "day_tendency_v1",
        "basis": "historical_expected_day",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bin_minutes": int(bin_minutes),
        "filters": {
            "dropped_first_last_dates": True,
            "min_daily_pickups_floor": 200,
            "min_daily_pickups_ratio": 0.2,
            "dropped_low_sample_dates": dropped_low_sample_dates,
        },
        "month_weekday": month_weekday,
        "weekday_only": weekday_only,
        "dataset": {
            "usable_dates": len(usable_rows),
            "first_date": first_date,
            "last_date": last_date,
        },
    }

    model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "model_path": str(model_path),
        "usable_dates": len(usable_rows),
        "month_weekday_cohorts": len(month_weekday),
        "weekday_only_cohorts": len(weekday_only),
    }
