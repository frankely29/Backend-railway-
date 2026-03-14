from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone
import bisect
import json

import duckdb


WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
CANONICAL_BOROUGHS = {
    "manhattan": "Manhattan",
    "brooklyn": "Brooklyn",
    "queens": "Queens",
    "bronx": "Bronx",
    "staten_island": "Staten Island",
    "newark_airport": "Newark Airport",
    "unknown": "Unknown",
}


def _borough_key_from_name(name: str) -> str:
    raw = (name or "").strip().lower().replace("-", " ").replace("_", " ")
    if raw == "manhattan":
        return "manhattan"
    if raw == "brooklyn":
        return "brooklyn"
    if raw == "queens":
        return "queens"
    if raw == "bronx":
        return "bronx"
    if raw in {"staten island", "statenisland"}:
        return "staten_island"
    if raw in {"newark airport", "newarkairport", "ewr", "newark"}:
        return "newark_airport"
    return "unknown"


def _canonical_borough(name: str) -> Tuple[str, str]:
    key = _borough_key_from_name(name)
    return CANONICAL_BOROUGHS.get(key, "Unknown"), key


def _load_zone_borough_map(zones_geojson_path: Path) -> Dict[int, Dict[str, str]]:
    out: Dict[int, Dict[str, str]] = {}
    try:
        raw = json.loads(zones_geojson_path.read_text(encoding="utf-8"))
        for feature in raw.get("features", []):
            props = feature.get("properties") or {}
            try:
                location_id = int(props.get("LocationID"))
            except Exception:
                continue
            borough_name, borough_key = _canonical_borough(str(props.get("borough") or ""))
            out[location_id] = {
                "borough": borough_name,
                "borough_key": borough_key,
            }
    except Exception:
        return {}
    return out


def percentile_rank(sorted_values: List[float], value: float) -> float:
    if not sorted_values:
        return 0.5
    if len(sorted_values) == 1:
        return 0.5
    left = bisect.bisect_left(sorted_values, value)
    right = bisect.bisect_right(sorted_values, value)
    avg_rank = (left + right - 1) / 2.0
    return max(0.0, min(1.0, avg_rank / (len(sorted_values) - 1)))


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


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


def _strength_relation(score_raw: float) -> str:
    if score_raw >= 0.65:
        return "stronger than most time blocks in this dataset"
    if score_raw <= 0.34:
        return "weaker than most time blocks in this dataset"
    return "near the middle of this dataset"


def _explain(cohort_type: str, item: Dict[str, Any]) -> str:
    relation = _strength_relation(float(item.get("score_raw", 0.5)))
    borough = str(item.get("borough") or "")
    weekday_name = str(item.get("weekday_name") or "")
    bin_label = str(item.get("bin_label") or "")

    if cohort_type == "borough_weekday_bin":
        return f"Typical {borough} {weekday_name}s around {bin_label} are {relation}."
    if cohort_type == "borough_bin":
        return f"Typical {borough} time blocks around {bin_label} are {relation}."
    if cohort_type == "borough_baseline":
        return f"Typical {borough} time blocks are {relation}."
    if cohort_type == "global_bin":
        return f"Typical time blocks around {bin_label} are {relation}."
    return f"Typical time blocks in this dataset are {relation}."


def _insufficient_payload(first_date: str | None = None, last_date: str | None = None, usable_dates: int = 0) -> Dict[str, Any]:
    return {
        "version": "borough_tendency_v1",
        "basis": "historical_expected_borough_timeslot",
        "bin_minutes": 20,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "insufficient_data",
        "borough_weekday_bin": {},
        "borough_bin": {},
        "borough_baseline": {},
        "global_bin": {},
        "global_baseline": {},
        "dataset": {
            "usable_dates": int(usable_dates),
            "first_date": first_date,
            "last_date": last_date,
        },
        "filters": {
            "dropped_first_last_dates": True,
            "min_daily_pickups_floor": 200,
            "min_daily_pickups_ratio": 0.2,
            "dropped_low_sample_dates": 0,
        },
    }


def build_day_tendency_model(
    parquet_files: List[Path],
    out_dir: Path,
    zones_geojson_path: Path,
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
            "borough_weekday_bin_cohorts": 0,
            "borough_bin_cohorts": 0,
        }

    zone_map = _load_zone_borough_map(zones_geojson_path)

    parquet_list = [str(p) for p in parquet_files]
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_list)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute(
        """
        CREATE TEMP TABLE zone_map (
            location_id INTEGER,
            borough VARCHAR,
            borough_key VARCHAR
        )
        """
    )
    if zone_map:
        rows_to_insert = [(int(k), v["borough"], v["borough_key"]) for k, v in zone_map.items()]
        con.executemany("INSERT INTO zone_map VALUES (?, ?, ?)", rows_to_insert)

    sql_daily = f"""
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
        b.PULocationID,
        CAST(b.pickup_datetime AS DATE) AS nyc_date,
        CASE
          WHEN CAST(EXTRACT('dow' FROM b.pickup_datetime) AS INTEGER) = 0 THEN 6
          ELSE CAST(EXTRACT('dow' FROM b.pickup_datetime) AS INTEGER) - 1
        END AS dow_m,
        CAST(EXTRACT('hour' FROM b.pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM b.pickup_datetime) AS INTEGER) AS minute_i,
        b.driver_pay,
        COALESCE(zm.borough, 'Unknown') AS borough,
        COALESCE(zm.borough_key, 'unknown') AS borough_key
      FROM base b
      LEFT JOIN zone_map zm ON zm.location_id = b.PULocationID
    )
    SELECT nyc_date, COUNT(*) AS daily_pickups
    FROM t
    GROUP BY 1
    ORDER BY nyc_date
    """
    daily_rows = con.execute(sql_daily).fetchall()

    sql_borough_bin = f"""
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
        b.PULocationID,
        CAST(b.pickup_datetime AS DATE) AS nyc_date,
        CASE
          WHEN CAST(EXTRACT('dow' FROM b.pickup_datetime) AS INTEGER) = 0 THEN 6
          ELSE CAST(EXTRACT('dow' FROM b.pickup_datetime) AS INTEGER) - 1
        END AS dow_m,
        CAST(FLOOR((CAST(EXTRACT('hour' FROM b.pickup_datetime) AS INTEGER) * 60 + CAST(EXTRACT('minute' FROM b.pickup_datetime) AS INTEGER)) / {int(bin_minutes)}) AS INTEGER) AS bin_index,
        b.driver_pay,
        COALESCE(zm.borough, 'Unknown') AS borough,
        COALESCE(zm.borough_key, 'unknown') AS borough_key
      FROM base b
      LEFT JOIN zone_map zm ON zm.location_id = b.PULocationID
    )
    SELECT
      nyc_date,
      dow_m,
      bin_index,
      borough,
      borough_key,
      COUNT(*) AS pickups_bin,
      AVG(driver_pay) AS avg_driver_pay_bin,
      COUNT(DISTINCT PULocationID) AS active_zones_bin
    FROM t
    GROUP BY 1,2,3,4,5
    ORDER BY nyc_date, bin_index, borough_key
    """
    rows = con.execute(sql_borough_bin).fetchall()
    con.close()

    if not rows or not daily_rows:
        payload = _insufficient_payload(usable_dates=0)
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "borough_weekday_bin_cohorts": 0,
            "borough_bin_cohorts": 0,
        }

    all_dates = [r[0] for r in daily_rows]
    first_date = str(min(all_dates)) if all_dates else None
    last_date = str(max(all_dates)) if all_dates else None

    daily_by_date = {r[0]: int(r[1]) for r in daily_rows}
    candidate_dates = sorted(daily_by_date.keys())
    if len(candidate_dates) >= 3:
        candidate_dates = candidate_dates[1:-1]

    median_daily = _median([float(daily_by_date[d]) for d in candidate_dates]) if candidate_dates else 0.0
    min_daily_pickups = max(200.0, 0.20 * float(median_daily))
    usable_dates = {d for d in candidate_dates if float(daily_by_date.get(d, 0)) >= min_daily_pickups}
    dropped_low_sample_dates = max(0, len(candidate_dates) - len(usable_dates))

    usable_rows = [r for r in rows if r[0] in usable_dates]

    if not usable_rows:
        payload = _insufficient_payload(first_date=first_date, last_date=last_date, usable_dates=0)
        payload["filters"]["dropped_low_sample_dates"] = int(dropped_low_sample_dates)
        model_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "ok": True,
            "model_path": str(model_path),
            "usable_dates": 0,
            "borough_weekday_bin_cohorts": 0,
            "borough_bin_cohorts": 0,
        }

    def build_cohorts(source_rows: List[Any], key_fn, cohort_type: str) -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Any]] = {}
        for r in source_rows:
            grouped.setdefault(key_fn(r), []).append(r)

        out: Dict[str, Dict[str, Any]] = {}
        for key, g in grouped.items():
            borough = str(g[0][3])
            borough_key = str(g[0][4])
            dow_m = int(g[0][1])
            bin_idx = int(g[0][2])
            item: Dict[str, Any] = {
                "borough": borough,
                "borough_key": borough_key,
                "sample_bins": len(g),
                "pickups_bin_avg": round(_mean([float(x[5]) for x in g]), 2),
                "avg_driver_pay_bin_avg": round(_mean([float(x[6]) if x[6] is not None else 0.0 for x in g]), 2),
                "active_zones_bin_avg": round(_mean([float(x[7]) for x in g]), 2),
                "cohort_type": cohort_type,
            }
            if cohort_type in {"borough_weekday_bin", "borough_bin", "global_bin"}:
                item["bin_index"] = bin_idx
                item["bin_label"] = _bin_label(bin_idx, int(bin_minutes))
            if cohort_type == "borough_weekday_bin":
                item["weekday"] = dow_m
                item["weekday_name"] = WEEKDAY_NAMES[dow_m]
            out[key] = item
        return out

    borough_weekday_bin = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{str(r[4])}|{int(r[1])}|{int(r[2])}",
        cohort_type="borough_weekday_bin",
    )
    borough_bin = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{str(r[4])}|{int(r[2])}",
        cohort_type="borough_bin",
    )
    borough_baseline = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{str(r[4])}",
        cohort_type="borough_baseline",
    )
    global_bin = build_cohorts(
        usable_rows,
        key_fn=lambda r: f"{int(r[2])}",
        cohort_type="global_bin",
    )

    def score_family(cohorts: Dict[str, Dict[str, Any]], cohort_type: str) -> None:
        pickup_values = sorted(float(v["pickups_bin_avg"]) for v in cohorts.values())
        pay_values = sorted(float(v["avg_driver_pay_bin_avg"]) for v in cohorts.values())
        breadth_values = sorted(float(v["active_zones_bin_avg"]) for v in cohorts.values())

        for v in cohorts.values():
            pickup_strength = percentile_rank(pickup_values, float(v["pickups_bin_avg"]))
            pay_strength = percentile_rank(pay_values, float(v["avg_driver_pay_bin_avg"]))
            breadth_strength = percentile_rank(breadth_values, float(v["active_zones_bin_avg"]))
            score_raw = 0.70 * pickup_strength + 0.15 * pay_strength + 0.15 * breadth_strength
            score = max(0, min(100, int(round(100 * score_raw))))
            band = _band_from_score(score)

            v["pickup_strength"] = round(pickup_strength, 4)
            v["pay_strength"] = round(pay_strength, 4)
            v["breadth_strength"] = round(breadth_strength, 4)
            v["score_raw"] = round(score_raw, 4)
            v["score"] = score
            v["band"] = band
            v["label"] = _label_from_band(band)
            v["confidence"] = round(min(1.0, float(v.get("sample_bins", 0)) / 12.0), 2)
            v["explain"] = _explain(cohort_type, v)

    score_family(borough_weekday_bin, "borough_weekday_bin")
    score_family(borough_bin, "borough_bin")
    score_family(borough_baseline, "borough_baseline")
    score_family(global_bin, "global_bin")

    global_pickups_avg = _mean([float(r[5]) for r in usable_rows])
    global_pay_avg = _mean([float(r[6]) if r[6] is not None else 0.0 for r in usable_rows])
    global_breadth_avg = _mean([float(r[7]) for r in usable_rows])

    global_bin_pickup_values = sorted(float(v["pickups_bin_avg"]) for v in global_bin.values())
    global_bin_pay_values = sorted(float(v["avg_driver_pay_bin_avg"]) for v in global_bin.values())
    global_bin_breadth_values = sorted(float(v["active_zones_bin_avg"]) for v in global_bin.values())

    pickup_strength = percentile_rank(global_bin_pickup_values, global_pickups_avg) if global_bin_pickup_values else 0.5
    pay_strength = percentile_rank(global_bin_pay_values, global_pay_avg) if global_bin_pay_values else 0.5
    breadth_strength = percentile_rank(global_bin_breadth_values, global_breadth_avg) if global_bin_breadth_values else 0.5
    score_raw = 0.70 * pickup_strength + 0.15 * pay_strength + 0.15 * breadth_strength
    score = max(0, min(100, int(round(100 * score_raw))))
    band = _band_from_score(score)

    global_baseline: Dict[str, Any] = {
        "sample_bins": len(usable_rows),
        "pickups_bin_avg": round(global_pickups_avg, 2),
        "avg_driver_pay_bin_avg": round(global_pay_avg, 2),
        "active_zones_bin_avg": round(global_breadth_avg, 2),
        "pickup_strength": round(pickup_strength, 4),
        "pay_strength": round(pay_strength, 4),
        "breadth_strength": round(breadth_strength, 4),
        "score_raw": round(score_raw, 4),
        "score": score,
        "band": band,
        "label": _label_from_band(band),
        "confidence": round(min(1.0, len(usable_rows) / 12.0), 2),
        "cohort_type": "global_baseline",
    }
    global_baseline["explain"] = _explain("global_baseline", global_baseline)

    payload = {
        "version": "borough_tendency_v1",
        "basis": "historical_expected_borough_timeslot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bin_minutes": int(bin_minutes),
        "filters": {
            "dropped_first_last_dates": True,
            "min_daily_pickups_floor": 200,
            "min_daily_pickups_ratio": 0.2,
            "dropped_low_sample_dates": int(dropped_low_sample_dates),
        },
        "borough_weekday_bin": borough_weekday_bin,
        "borough_bin": borough_bin,
        "borough_baseline": borough_baseline,
        "global_bin": global_bin,
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
        "borough_weekday_bin_cohorts": len(borough_weekday_bin),
        "borough_bin_cohorts": len(borough_bin),
    }
