from __future__ import annotations

from typing import Iterable, Optional, Set

from zone_mode_profiles import ZoneScoreProfileWeights

BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS_SQL = "41, 42, 74, 75, 116, 127, 128, 151, 152, 166, 243, 244"


def clip01(value_sql: str) -> str:
    return f"LEAST(GREATEST(({value_sql}), 0.0), 1.0)"


def percentile_rank_expr(value_expr: str, partition_expr: str, alias_prefix: str) -> str:
    return f"""
    ROW_NUMBER() OVER (PARTITION BY {partition_expr} ORDER BY {value_expr}) AS {alias_prefix}_rn,
    COUNT(*) OVER (PARTITION BY {partition_expr}) AS {alias_prefix}_n
    """


def nullable_percentile_rank_expr(value_expr: str, partition_expr: str, alias_prefix: str) -> str:
    return f"""
    CASE
      WHEN {value_expr} IS NULL THEN NULL
      ELSE ROW_NUMBER() OVER (
        PARTITION BY {partition_expr}, CASE WHEN {value_expr} IS NULL THEN 0 ELSE 1 END
        ORDER BY {value_expr}
      )
    END AS {alias_prefix}_rn,
    COUNT({value_expr}) OVER (PARTITION BY {partition_expr}) AS {alias_prefix}_n
    """


def safe_div_sql(numerator: str, denominator: str, fallback: str = "0.0") -> str:
    return f"COALESCE(({numerator}) / NULLIF(({denominator}), 0), {fallback})"


def nullable_weighted_average_sql(weight_expr_pairs: Iterable[tuple[str, str]], fallback: str = "0.0") -> str:
    pairs = list(weight_expr_pairs)
    if not pairs:
        return fallback
    numerator = " + ".join(
        f"(CASE WHEN {expr_sql} IS NULL THEN 0.0 ELSE ({weight_sql}) * ({expr_sql}) END)"
        for weight_sql, expr_sql in pairs
    )
    denominator = " + ".join(
        f"(CASE WHEN {expr_sql} IS NULL THEN 0.0 ELSE ({weight_sql}) END)"
        for weight_sql, expr_sql in pairs
    )
    return f"CASE WHEN ({denominator}) <= 0 THEN {fallback} ELSE ({numerator}) / ({denominator}) END"


def minute_diff_sql(end_ts_expr: str, start_ts_expr: str) -> str:
    return f"(EXTRACT(EPOCH FROM ({end_ts_expr} - {start_ts_expr})) / 60.0)"


def build_zone_earnings_shadow_sql(
    parquet_files: Iterable[str],
    *,
    bin_minutes: int,
    min_trips_per_window: int,
    profile: ZoneScoreProfileWeights,
    citywide_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    manhattan_profile: Optional[ZoneScoreProfileWeights] = None,
    bronx_wash_heights_profile: Optional[ZoneScoreProfileWeights] = None,
    queens_profile: Optional[ZoneScoreProfileWeights] = None,
    brooklyn_profile: Optional[ZoneScoreProfileWeights] = None,
    staten_island_profile: Optional[ZoneScoreProfileWeights] = None,
    manhattan_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    bronx_wash_heights_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    queens_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    brooklyn_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    staten_island_v3_profile: Optional[ZoneScoreProfileWeights] = None,
    available_columns: Optional[Set[str]] = None,
) -> str:
    parquet_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in parquet_files)
    bins_per_day = int(1440 // bin_minutes)
    bins_per_week = 7 * bins_per_day

    cols = {c.lower() for c in (available_columns or set())}
    has_request_datetime = "request_datetime" in cols if cols else True
    has_shared_match_flag = "shared_match_flag" in cols if cols else False
    has_shared_request_flag = "shared_request_flag" in cols if cols else False

    lag_min_expr = minute_diff_sql("pickup_datetime", "request_datetime")
    pay_per_min_expr = safe_div_sql("driver_pay", "GREATEST(trip_time / 60.0, 1.0 / 60.0)", "NULL")
    pay_per_mile_expr = safe_div_sql("driver_pay", "GREATEST(trip_miles, 0.1)", "NULL")
    shared_expr_parts = []
    if has_shared_match_flag:
        shared_expr_parts.append("TRY_CAST(shared_match_flag AS INTEGER)")
    if has_shared_request_flag:
        shared_expr_parts.append("TRY_CAST(shared_request_flag AS INTEGER)")
    shared_expr = "COALESCE(" + ", ".join(shared_expr_parts + ["0"]) + ")"

    w = profile
    c3w = citywide_v3_profile or profile
    mw = manhattan_profile or profile
    bw = bronx_wash_heights_profile or profile
    qw = queens_profile or profile
    bkw = brooklyn_profile or profile
    sw = staten_island_profile or profile
    mw3 = manhattan_v3_profile or mw
    bw3 = bronx_wash_heights_v3_profile or bw
    qw3 = queens_v3_profile or qw
    bkw3 = brooklyn_v3_profile or bkw
    sw3 = staten_island_v3_profile or sw
    c3_busy_now_weight = c3w.demand_now_weight + c3w.demand_density_now_weight
    c3_busy_next_weight = c3w.demand_next_weight + c3w.demand_density_next_weight
    mw3_busy_now_weight = mw3.demand_now_weight + mw3.demand_density_now_weight
    mw3_busy_next_weight = mw3.demand_next_weight + mw3.demand_density_next_weight
    bw3_busy_now_weight = bw3.demand_now_weight + bw3.demand_density_now_weight
    bw3_busy_next_weight = bw3.demand_next_weight + bw3.demand_density_next_weight
    qw3_busy_now_weight = qw3.demand_now_weight + qw3.demand_density_now_weight
    qw3_busy_next_weight = qw3.demand_next_weight + qw3.demand_density_next_weight
    bkw3_busy_now_weight = bkw3.demand_now_weight + bkw3.demand_density_now_weight
    bkw3_busy_next_weight = bkw3.demand_next_weight + bkw3.demand_density_next_weight
    sw3_busy_now_weight = sw3.demand_now_weight + sw3.demand_density_now_weight
    sw3_busy_next_weight = sw3.demand_next_weight + sw3.demand_density_next_weight
    manhattan_core_citywide_guard_sql = f"""
POSITION('manhattan' IN LOWER(COALESCE(borough_name, ''))) > 0
AND COALESCE(centroid_latitude, 999.0) <= 40.795
AND PULocationID NOT IN ({BRONX_WASH_HEIGHTS_CORRIDOR_ZONE_IDS_SQL})
""".strip()

    return f"""
    WITH base AS (
      SELECT
        CAST(PULocationID AS INTEGER) AS PULocationID,
        CAST(DOLocationID AS INTEGER) AS DOLocationID,
        pickup_datetime,
        {"request_datetime," if has_request_datetime else "NULL::TIMESTAMP AS request_datetime,"}
        TRY_CAST(driver_pay AS DOUBLE) AS driver_pay,
        TRY_CAST(trip_time AS DOUBLE) AS trip_time,
        TRY_CAST(trip_miles AS DOUBLE) AS trip_miles,
        {shared_expr} AS shared_flag
      FROM read_parquet([{parquet_sql}])
      WHERE PULocationID IS NOT NULL
        AND CAST(PULocationID AS INTEGER) IN (
          SELECT PULocationID FROM zone_metadata WHERE airport_excluded = FALSE
        )
        AND pickup_datetime IS NOT NULL
    ),
    prepared AS (
      SELECT
        PULocationID,
        DOLocationID,
        CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,
        CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
        CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
        driver_pay,
        trip_time,
        trip_miles,
        shared_flag,
        CASE
          WHEN request_datetime IS NULL THEN NULL
          ELSE {lag_min_expr}
        END AS request_to_pickup_min,
        {pay_per_min_expr} AS pay_per_min,
        {pay_per_mile_expr} AS pay_per_mile
      FROM base
      WHERE PULocationID IS NOT NULL
    ),
    binned AS (
      SELECT
        PULocationID,
        DOLocationID,
        CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,
        CAST(FLOOR((hour_i * 60 + minute_i) / {bin_minutes}) * {bin_minutes} AS INTEGER) AS bin_start_min,
        driver_pay,
        trip_time,
        trip_miles,
        pay_per_min,
        pay_per_mile,
        request_to_pickup_min,
        CASE WHEN trip_miles <= 3.0 AND trip_time <= 720.0 THEN 1 ELSE 0 END AS is_short_trip,
        CASE WHEN shared_flag = 1 THEN 1 ELSE 0 END AS is_shared,
        CASE WHEN trip_time >= 1200 THEN 1 ELSE 0 END AS is_long_trip_20plus,
        CASE WHEN trip_miles BETWEEN 3.0 AND 10.0 AND trip_time BETWEEN 720.0 AND 2400.0 THEN 1 ELSE 0 END AS is_balanced_trip,
        CASE WHEN DOLocationID = PULocationID THEN 1 ELSE 0 END AS is_same_zone_dropoff
      FROM prepared
      WHERE PULocationID IS NOT NULL
    ),
    zone_bin_raw AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS pickups_now,
        MEDIAN(driver_pay) AS median_driver_pay,
        MEDIAN(pay_per_min) AS median_pay_per_min,
        MEDIAN(pay_per_mile) AS median_pay_per_mile,
        MEDIAN(CASE WHEN request_to_pickup_min >= 0 THEN request_to_pickup_min END) AS median_request_to_pickup_min,
        AVG(is_short_trip * 1.0) AS short_trip_share_3mi_12min,
        AVG(is_shared * 1.0) AS shared_ride_share,
        AVG(is_long_trip_20plus * 1.0) AS long_trip_share_20plus,
        AVG(is_balanced_trip * 1.0) AS balanced_trip_share,
        AVG(is_same_zone_dropoff * 1.0) AS same_zone_dropoff_share
      FROM binned
      GROUP BY 1,2,3
      HAVING COUNT(*) >= {min_trips_per_window}
    ),
    with_next AS (
      SELECT
        z.*,
        LEAD(pickups_now) OVER (PARTITION BY PULocationID, dow_m ORDER BY bin_start_min) AS pickups_next_same_day
      FROM zone_bin_raw z
    ),
    zone_bin AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        pickups_now,
        COALESCE(
          pickups_next_same_day,
          FIRST_VALUE(pickups_now) OVER (PARTITION BY PULocationID, dow_m ORDER BY bin_start_min),
          pickups_now
        ) AS pickups_next,
        median_driver_pay,
        median_pay_per_min,
        median_pay_per_mile,
        COALESCE(median_request_to_pickup_min, 0.0) AS median_request_to_pickup_min,
        COALESCE(short_trip_share_3mi_12min, 0.0) AS short_trip_share_3mi_12min,
        COALESCE(shared_ride_share, 0.0) AS shared_ride_share,
        COALESCE(long_trip_share_20plus, 0.0) AS long_trip_share_20plus,
        COALESCE(balanced_trip_share, 0.0) AS balanced_trip_share,
        COALESCE(same_zone_dropoff_share, 0.0) AS same_zone_dropoff_share,
        (dow_m * {bins_per_day} + CAST(bin_start_min / {bin_minutes} AS INTEGER)) AS bin_index
      FROM with_next
    ),
    dest_edges AS (
      SELECT
        PULocationID,
        DOLocationID,
        dow_m,
        bin_start_min,
        COUNT(*) AS edge_trips
      FROM binned
      WHERE DOLocationID IS NOT NULL
        AND DOLocationID IN (
          SELECT PULocationID FROM zone_metadata WHERE airport_excluded = FALSE
        )
      GROUP BY 1,2,3,4
    ),
    dest_edges_norm AS (
      SELECT
        e.*,
        SUM(edge_trips) OVER (PARTITION BY PULocationID, dow_m, bin_start_min) AS edge_total,
        (dow_m * {bins_per_day} + CAST(bin_start_min / {bin_minutes} AS INTEGER)) AS src_index,
        ((dow_m * {bins_per_day} + CAST(bin_start_min / {bin_minutes} AS INTEGER) + 1) % {bins_per_week}) AS dst_index
      FROM dest_edges e
    ),
    zone_bin_by_index AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        pickups_now,
        median_driver_pay,
        median_pay_per_min,
        (dow_m * {bins_per_day} + CAST(bin_start_min / {bin_minutes} AS INTEGER)) AS idx
      FROM zone_bin
    ),
    downstream_scores AS (
      SELECT
        d.PULocationID,
        d.dow_m,
        d.bin_start_min,
        SUM(
          {safe_div_sql('d.edge_trips', 'd.edge_total')} *
          {nullable_weighted_average_sql([
            ("0.50", "LN(1 + COALESCE(z.pickups_now, 0))"),
            ("0.35", "z.median_driver_pay"),
            ("0.15", "z.median_pay_per_min"),
          ])}
        ) AS downstream_next_value_raw,
        SUM(CASE WHEN z.PULocationID IS NULL THEN 0 ELSE d.edge_trips END) * 1.0 / NULLIF(SUM(d.edge_trips), 0) AS downstream_coverage
      FROM dest_edges_norm d
      LEFT JOIN zone_bin_by_index z
        ON z.PULocationID = d.DOLocationID
       AND z.idx = d.dst_index
      GROUP BY 1,2,3
    ),
    joined AS (
      SELECT
        z.PULocationID,
        z.dow_m,
        z.bin_start_min,
        z.pickups_now,
        z.pickups_next,
        z.median_driver_pay,
        z.median_pay_per_min,
        z.median_pay_per_mile,
        z.median_request_to_pickup_min,
        z.short_trip_share_3mi_12min,
        z.shared_ride_share,
        z.long_trip_share_20plus,
        z.balanced_trip_share,
        z.same_zone_dropoff_share,
        COALESCE(d.downstream_next_value_raw, 0.0) AS downstream_next_value_raw,
        COALESCE(d.downstream_coverage, 0.0) AS downstream_coverage,
        g.zone_area_sq_miles,
        g.centroid_latitude,
        m.borough_name,
        CASE
          WHEN g.zone_area_sq_miles > 0 THEN z.pickups_now * 1.0 / g.zone_area_sq_miles
          ELSE NULL
        END AS pickups_per_sq_mile_now,
        CASE
          WHEN g.zone_area_sq_miles > 0 THEN z.pickups_next * 1.0 / g.zone_area_sq_miles
          ELSE NULL
        END AS pickups_per_sq_mile_next
      FROM zone_bin z
      LEFT JOIN downstream_scores d
        ON d.PULocationID = z.PULocationID
       AND d.dow_m = z.dow_m
       AND d.bin_start_min = z.bin_start_min
      LEFT JOIN zone_geometry_metrics g
        ON g.PULocationID = z.PULocationID
      LEFT JOIN zone_metadata m
        ON m.PULocationID = z.PULocationID
    ),
    ranked AS (
      SELECT
        *,
        {percentile_rank_expr('LN(1 + pickups_now)', 'dow_m, bin_start_min', 'demand_now')},
        {percentile_rank_expr('LN(1 + pickups_next)', 'dow_m, bin_start_min', 'demand_next')},
        {nullable_percentile_rank_expr('median_driver_pay', 'dow_m, bin_start_min', 'pay')},
        {nullable_percentile_rank_expr('median_pay_per_min', 'dow_m, bin_start_min', 'pay_per_min')},
        {nullable_percentile_rank_expr('median_pay_per_mile', 'dow_m, bin_start_min', 'pay_per_mile')},
        {percentile_rank_expr('median_request_to_pickup_min', 'dow_m, bin_start_min', 'pickup_friction_penalty')},
        {percentile_rank_expr('short_trip_share_3mi_12min', 'dow_m, bin_start_min', 'short_trip_penalty')},
        {percentile_rank_expr('shared_ride_share', 'dow_m, bin_start_min', 'shared_ride_penalty')},
        {percentile_rank_expr('downstream_next_value_raw', 'dow_m, bin_start_min', 'downstream_value')},
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY pickups_per_sq_mile_now) AS demand_density_now_rn,
        COUNT(pickups_per_sq_mile_now) OVER (PARTITION BY dow_m, bin_start_min) AS demand_density_now_n,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY pickups_per_sq_mile_next) AS demand_density_next_rn,
        COUNT(pickups_per_sq_mile_next) OVER (PARTITION BY dow_m, bin_start_min) AS demand_density_next_n,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY long_trip_share_20plus) AS long_trip_share_20plus_rn,
        COUNT(long_trip_share_20plus) OVER (PARTITION BY dow_m, bin_start_min) AS long_trip_share_20plus_n,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY balanced_trip_share) AS balanced_trip_share_rn,
        COUNT(balanced_trip_share) OVER (PARTITION BY dow_m, bin_start_min) AS balanced_trip_share_n,
        ROW_NUMBER() OVER (PARTITION BY dow_m, bin_start_min ORDER BY same_zone_dropoff_share) AS same_zone_retention_penalty_rn,
        COUNT(same_zone_dropoff_share) OVER (PARTITION BY dow_m, bin_start_min) AS same_zone_retention_penalty_n
      FROM joined
    ),
    normalized AS (
      SELECT
        PULocationID,
        dow_m,
        bin_start_min,
        pickups_now,
        pickups_next,
        median_driver_pay,
        median_pay_per_min,
        median_pay_per_mile,
        median_request_to_pickup_min,
        short_trip_share_3mi_12min,
        shared_ride_share,
        long_trip_share_20plus,
        balanced_trip_share,
        same_zone_dropoff_share,
        zone_area_sq_miles,
        centroid_latitude,
        borough_name,
        pickups_per_sq_mile_now,
        pickups_per_sq_mile_next,
        downstream_next_value_raw,
        CASE WHEN demand_now_n <= 1 THEN 0.0 ELSE (demand_now_rn - 1) * 1.0 / (demand_now_n - 1) END AS demand_now_n,
        CASE WHEN demand_next_n <= 1 THEN 0.0 ELSE (demand_next_rn - 1) * 1.0 / (demand_next_n - 1) END AS demand_next_n,
        CASE
          WHEN median_driver_pay IS NULL THEN NULL
          WHEN pay_n <= 1 THEN 0.0
          ELSE (pay_rn - 1) * 1.0 / (pay_n - 1)
        END AS pay_n,
        CASE
          WHEN median_pay_per_min IS NULL THEN NULL
          WHEN pay_per_min_n <= 1 THEN 0.0
          ELSE (pay_per_min_rn - 1) * 1.0 / (pay_per_min_n - 1)
        END AS pay_per_min_n,
        CASE
          WHEN median_pay_per_mile IS NULL THEN NULL
          WHEN pay_per_mile_n <= 1 THEN 0.0
          ELSE (pay_per_mile_rn - 1) * 1.0 / (pay_per_mile_n - 1)
        END AS pay_per_mile_n,
        CASE WHEN pickup_friction_penalty_n <= 1 THEN 0.0 ELSE (pickup_friction_penalty_rn - 1) * 1.0 / (pickup_friction_penalty_n - 1) END AS pickup_friction_penalty_n,
        CASE WHEN short_trip_penalty_n <= 1 THEN 0.0 ELSE (short_trip_penalty_rn - 1) * 1.0 / (short_trip_penalty_n - 1) END AS short_trip_penalty_n,
        CASE WHEN shared_ride_penalty_n <= 1 THEN 0.0 ELSE (shared_ride_penalty_rn - 1) * 1.0 / (shared_ride_penalty_n - 1) END AS shared_ride_penalty_n,
        CASE WHEN downstream_value_n <= 1 THEN 0.0 ELSE (downstream_value_rn - 1) * 1.0 / (downstream_value_n - 1) END AS downstream_value_n,
        CASE
          WHEN pickups_per_sq_mile_now IS NULL THEN NULL
          WHEN demand_density_now_n <= 1 THEN 0.0
          ELSE (demand_density_now_rn - 1) * 1.0 / (demand_density_now_n - 1)
        END AS demand_density_now_n,
        CASE
          WHEN pickups_per_sq_mile_next IS NULL THEN NULL
          WHEN demand_density_next_n <= 1 THEN 0.0
          ELSE (demand_density_next_rn - 1) * 1.0 / (demand_density_next_n - 1)
        END AS demand_density_next_n,
        CASE
          WHEN long_trip_share_20plus IS NULL THEN NULL
          WHEN long_trip_share_20plus_n <= 1 THEN 0.0
          ELSE (long_trip_share_20plus_rn - 1) * 1.0 / (long_trip_share_20plus_n - 1)
        END AS long_trip_share_20plus_n,
        CASE
          WHEN same_zone_dropoff_share IS NULL THEN NULL
          WHEN same_zone_retention_penalty_n <= 1 THEN 0.0
          ELSE (same_zone_retention_penalty_rn - 1) * 1.0 / (same_zone_retention_penalty_n - 1)
        END AS same_zone_retention_penalty_n,
        CASE
          WHEN balanced_trip_share IS NULL THEN NULL
          WHEN balanced_trip_share_n <= 1 THEN 0.0
          ELSE (balanced_trip_share_rn - 1) * 1.0 / (balanced_trip_share_n - 1)
        END AS balanced_trip_share_n,
        downstream_coverage
      FROM ranked
    ),
    normalized_support AS (
      SELECT
        *,
        GREATEST(demand_now_n, demand_next_n) AS demand_support_n,
        LEAST(
          GREATEST((0.20 + 0.80 * GREATEST(demand_now_n, demand_next_n)), 0.0),
          1.0
        ) AS density_support_n,
        COALESCE(demand_density_now_n, demand_now_n) * LEAST(
          GREATEST((0.20 + 0.80 * GREATEST(demand_now_n, demand_next_n)), 0.0),
          1.0
        ) AS effective_demand_density_now_n,
        COALESCE(demand_density_next_n, demand_next_n) * LEAST(
          GREATEST((0.20 + 0.80 * GREATEST(demand_now_n, demand_next_n)), 0.0),
          1.0
        ) AS effective_demand_density_next_n,
        LEAST(GREATEST((0.68 * demand_now_n + 0.32 * (COALESCE(demand_density_now_n, demand_now_n) * LEAST(GREATEST((0.20 + 0.80 * GREATEST(demand_now_n, demand_next_n)), 0.0), 1.0))), 0.0), 1.0) AS busy_now_base_n,
        LEAST(GREATEST((0.62 * demand_next_n + 0.38 * (COALESCE(demand_density_next_n, demand_next_n) * LEAST(GREATEST((0.20 + 0.80 * GREATEST(demand_now_n, demand_next_n)), 0.0), 1.0))), 0.0), 1.0) AS busy_next_base_n,
        LEAST(
          GREATEST(
            (
              0.30 * effective_demand_density_now_n +
              0.15 * demand_support_n +
              0.18 * short_trip_penalty_n +
              0.16 * COALESCE(same_zone_retention_penalty_n, 0.0) +
              0.11 * (1.0 - COALESCE(pay_per_mile_n, 0.5)) +
              0.10 * (1.0 - downstream_value_n)
            ),
            0.0
          ),
          1.0
        ) AS market_saturation_pressure_n,
        CASE
          WHEN lower(coalesce(borough_name, '')) LIKE '%manhattan%' THEN 1.00
          WHEN lower(coalesce(borough_name, '')) LIKE '%brooklyn%' THEN 0.55
          WHEN lower(coalesce(borough_name, '')) LIKE '%queens%' THEN 0.50
          WHEN lower(coalesce(borough_name, '')) LIKE '%bronx%' THEN 0.40
          WHEN lower(coalesce(borough_name, '')) LIKE '%staten%' THEN 0.20
          ELSE 0.35
        END AS borough_multiplier,
        LEAST(
          GREATEST(
            (
              0.45 * COALESCE(same_zone_retention_penalty_n, 0.0) +
              0.35 * short_trip_penalty_n +
              0.20 * GREATEST(COALESCE(demand_density_now_n, 0.0) - GREATEST(COALESCE(demand_next_n, 0.0), COALESCE(downstream_value_n, 0.0)), 0.0)
            ),
            0.0
          ),
          1.0
        ) AS churn_pressure_n,
        CASE
          WHEN {manhattan_core_citywide_guard_sql}
          THEN LEAST(
            GREATEST(
              (
                0.45 * COALESCE(same_zone_retention_penalty_n, 0.0) +
                0.35 * short_trip_penalty_n +
                0.20 * GREATEST(COALESCE(demand_density_now_n, 0.0) - GREATEST(COALESCE(demand_next_n, 0.0), COALESCE(downstream_value_n, 0.0)), 0.0)
              ),
              0.0
            ),
            1.0
          )
          ELSE 0.0
        END AS manhattan_core_saturation_proxy_n,
        CASE
          WHEN {manhattan_core_citywide_guard_sql}
          THEN LEAST(
            GREATEST(
              (
                0.55 * LEAST(
                  GREATEST(
                    (
                      0.45 * COALESCE(same_zone_retention_penalty_n, 0.0) +
                      0.35 * short_trip_penalty_n +
                      0.20 * GREATEST(COALESCE(demand_density_now_n, 0.0) - GREATEST(COALESCE(demand_next_n, 0.0), COALESCE(downstream_value_n, 0.0)), 0.0)
                    ),
                    0.0
                  ),
                  1.0
                ) +
                0.45 * LEAST(
                  GREATEST(
                    (
                      0.45 * COALESCE(same_zone_retention_penalty_n, 0.0) +
                      0.35 * short_trip_penalty_n +
                      0.20 * GREATEST(COALESCE(demand_density_now_n, 0.0) - GREATEST(COALESCE(demand_next_n, 0.0), COALESCE(downstream_value_n, 0.0)), 0.0)
                    ),
                    0.0
                  ),
                  1.0
                )
              ),
              0.0
            ),
            1.0
          )
          ELSE 0.0
        END AS manhattan_core_saturation_penalty_n,
        CASE
          WHEN {manhattan_core_citywide_guard_sql}
          THEN LEAST(GREATEST(1.0 - (0.14 * manhattan_core_saturation_penalty_n), 0.86), 1.00)
          ELSE 1.00
        END AS citywide_manhattan_saturation_discount_factor_n,
        CASE
          WHEN {manhattan_core_citywide_guard_sql}
          THEN LEAST(
            GREATEST(
              (
                0.52 * short_trip_penalty_n +
                0.30 * COALESCE(same_zone_retention_penalty_n, 0.0) +
                0.18 * GREATEST(
                  COALESCE(demand_density_now_n, 0.0) -
                  GREATEST(COALESCE(downstream_value_n, 0.0), COALESCE(busy_next_base_n, 0.0)),
                  0.0
                )
              ),
              0.0
            ),
            1.0
          )
          ELSE 0.0
        END AS citywide_manhattan_short_trip_trap_penalty_n,
        CASE
          WHEN {manhattan_core_citywide_guard_sql}
          THEN LEAST(
            GREATEST(
              (
                0.52 * COALESCE(balanced_trip_share_n, 0.0) +
                0.28 * COALESCE(long_trip_share_20plus_n, 0.0) +
                0.20 * COALESCE(downstream_value_n, 0.0)
              ),
              0.0
            ),
            1.0
          )
          ELSE 0.0
        END AS citywide_manhattan_escape_bonus_n
      FROM normalized
    ),
    normalized_support_enriched AS (
      SELECT
        *,
        LEAST(
          GREATEST(
            (
              market_saturation_pressure_n * borough_multiplier
            ),
            0.0
          ),
          1.0
        ) AS market_saturation_penalty_n
      FROM normalized_support
    ),
    scored AS (
      SELECT
        *,
        {nullable_weighted_average_sql([
          (f"{w.demand_now_weight:.8f}", "demand_now_n"),
          (f"{w.demand_next_weight:.8f}", "demand_next_n"),
          (f"{w.pay_weight:.8f}", "pay_n"),
          (f"{w.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{w.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{w.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score,
        (
          {w.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {w.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {w.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score,
        {nullable_weighted_average_sql([
          (f"{mw.demand_now_weight:.8f}", "demand_now_n"),
          (f"{mw.demand_next_weight:.8f}", "demand_next_n"),
          (f"{mw.pay_weight:.8f}", "pay_n"),
          (f"{mw.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{mw.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{mw.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_manhattan_v2,
        (
          {mw.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {mw.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {mw.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score_manhattan_v2,
        {nullable_weighted_average_sql([
          (f"{bw.demand_now_weight:.8f}", "demand_now_n"),
          (f"{bw.demand_next_weight:.8f}", "demand_next_n"),
          (f"{bw.pay_weight:.8f}", "pay_n"),
          (f"{bw.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{bw.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{bw.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_bronx_wash_heights_v2,
        (
          {bw.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {bw.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {bw.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score_bronx_wash_heights_v2,
        {nullable_weighted_average_sql([
          (f"{qw.demand_now_weight:.8f}", "demand_now_n"),
          (f"{qw.demand_next_weight:.8f}", "demand_next_n"),
          (f"{qw.pay_weight:.8f}", "pay_n"),
          (f"{qw.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{qw.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{qw.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_queens_v2,
        (
          {qw.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {qw.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {qw.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score_queens_v2,
        {nullable_weighted_average_sql([
          (f"{bkw.demand_now_weight:.8f}", "demand_now_n"),
          (f"{bkw.demand_next_weight:.8f}", "demand_next_n"),
          (f"{bkw.pay_weight:.8f}", "pay_n"),
          (f"{bkw.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{bkw.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{bkw.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_brooklyn_v2,
        (
          {bkw.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {bkw.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {bkw.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score_brooklyn_v2,
        {nullable_weighted_average_sql([
          (f"{sw.demand_now_weight:.8f}", "demand_now_n"),
          (f"{sw.demand_next_weight:.8f}", "demand_next_n"),
          (f"{sw.pay_weight:.8f}", "pay_n"),
          (f"{sw.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{sw.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{sw.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_staten_island_v2,
        (
          {sw.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {sw.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {sw.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n
        ) AS negative_score_staten_island_v2,
        {nullable_weighted_average_sql([
          (f"{c3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{c3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{c3w.pay_weight:.8f}", "pay_n"),
          (f"{c3w.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{c3w.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{c3w.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{c3w.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{c3w.downstream_weight:.8f}", "downstream_value_n"),
        ])}
        + CASE
            WHEN {manhattan_core_citywide_guard_sql}
            THEN 0.045 * citywide_manhattan_escape_bonus_n
            ELSE 0.0
          END AS positive_score_citywide_v3,
        (
          {c3w.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {c3w.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {c3w.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {c3w.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {c3w.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0) +
          CASE
            WHEN {manhattan_core_citywide_guard_sql}
            THEN
              0.060 * manhattan_core_saturation_penalty_n +
              0.070 * citywide_manhattan_short_trip_trap_penalty_n
            ELSE 0.0
          END
        ) AS negative_score_citywide_v3,
        {nullable_weighted_average_sql([
          (f"{mw3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{mw3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{mw3.pay_weight:.8f}", "pay_n"),
          (f"{mw3.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{mw3.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{mw3.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{mw3.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{mw3.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_manhattan_v3,
        (
          {mw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {mw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {mw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {mw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {mw3.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0) +
          0.10 * manhattan_core_saturation_penalty_n
        ) AS negative_score_manhattan_v3,
        {nullable_weighted_average_sql([
          (f"{bw3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{bw3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{bw3.pay_weight:.8f}", "pay_n"),
          (f"{bw3.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{bw3.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{bw3.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{bw3.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{bw3.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_bronx_wash_heights_v3,
        (
          {bw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {bw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {bw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {bw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {bw3.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0)
        ) AS negative_score_bronx_wash_heights_v3,
        {nullable_weighted_average_sql([
          (f"{qw3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{qw3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{qw3.pay_weight:.8f}", "pay_n"),
          (f"{qw3.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{qw3.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{qw3.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{qw3.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{qw3.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_queens_v3,
        (
          {qw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {qw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {qw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {qw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {qw3.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0)
        ) AS negative_score_queens_v3,
        {nullable_weighted_average_sql([
          (f"{bkw3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{bkw3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{bkw3.pay_weight:.8f}", "pay_n"),
          (f"{bkw3.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{bkw3.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{bkw3.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{bkw3.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{bkw3.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_brooklyn_v3,
        (
          {bkw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {bkw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {bkw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {bkw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {bkw3.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0)
        ) AS negative_score_brooklyn_v3,
        {nullable_weighted_average_sql([
          (f"{sw3_busy_now_weight:.8f}", "busy_now_base_n"),
          (f"{sw3_busy_next_weight:.8f}", "busy_next_base_n"),
          (f"{sw3.pay_weight:.8f}", "pay_n"),
          (f"{sw3.pay_per_min_weight:.8f}", "pay_per_min_n"),
          (f"{sw3.pay_per_mile_weight:.8f}", "pay_per_mile_n"),
          (f"{sw3.balanced_trip_share_weight:.8f}", "COALESCE(balanced_trip_share_n, 0.0)"),
          (f"{sw3.long_trip_share_20plus_weight:.8f}", "COALESCE(long_trip_share_20plus_n, 0.0)"),
          (f"{sw3.downstream_weight:.8f}", "downstream_value_n"),
        ])} AS positive_score_staten_island_v3,
        (
          {sw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n +
          {sw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0) +
          {sw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n +
          {sw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n +
          {sw3.market_saturation_penalty_weight:.8f} * COALESCE(market_saturation_penalty_n, 0.0)
        ) AS negative_score_staten_island_v3,
        LEAST(1.0, pickups_now / 40.0) * (0.70 + 0.30 * downstream_coverage) AS earnings_shadow_confidence_citywide_v2
      FROM normalized_support_enriched
    ),
    final AS (
      SELECT
        *,
        {clip01('positive_score - negative_score')} AS shadow_score_raw,
        (positive_score_citywide_v3 - negative_score_citywide_v3) AS shadow_score_raw_citywide_v3,
        {clip01('positive_score_manhattan_v2 - negative_score_manhattan_v2')} AS shadow_score_raw_manhattan_v2,
        {clip01('positive_score_bronx_wash_heights_v2 - negative_score_bronx_wash_heights_v2')} AS shadow_score_raw_bronx_wash_heights_v2,
        {clip01('positive_score_queens_v2 - negative_score_queens_v2')} AS shadow_score_raw_queens_v2,
        {clip01('positive_score_brooklyn_v2 - negative_score_brooklyn_v2')} AS shadow_score_raw_brooklyn_v2,
        {clip01('positive_score_staten_island_v2 - negative_score_staten_island_v2')} AS shadow_score_raw_staten_island_v2,
        {clip01('positive_score_manhattan_v3 - negative_score_manhattan_v3')} AS shadow_score_raw_manhattan_v3,
        {clip01('positive_score_bronx_wash_heights_v3 - negative_score_bronx_wash_heights_v3')} AS shadow_score_raw_bronx_wash_heights_v3,
        {clip01('positive_score_queens_v3 - negative_score_queens_v3')} AS shadow_score_raw_queens_v3,
        {clip01('positive_score_brooklyn_v3 - negative_score_brooklyn_v3')} AS shadow_score_raw_brooklyn_v3,
        {clip01('positive_score_staten_island_v3 - negative_score_staten_island_v3')} AS shadow_score_raw_staten_island_v3,
        {clip01(f"{clip01('positive_score - negative_score')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_citywide_v2,
        earnings_shadow_confidence_citywide_v2 AS earnings_shadow_confidence_manhattan_v2,
        {clip01(f"{clip01('positive_score_manhattan_v2 - negative_score_manhattan_v2')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_manhattan_v2,
        earnings_shadow_confidence_citywide_v2 AS earnings_shadow_confidence_bronx_wash_heights_v2,
        {clip01(f"{clip01('positive_score_bronx_wash_heights_v2 - negative_score_bronx_wash_heights_v2')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_bronx_wash_heights_v2,
        earnings_shadow_confidence_citywide_v2 AS earnings_shadow_confidence_queens_v2,
        {clip01(f"{clip01('positive_score_queens_v2 - negative_score_queens_v2')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_queens_v2,
        earnings_shadow_confidence_citywide_v2 AS earnings_shadow_confidence_brooklyn_v2,
        {clip01(f"{clip01('positive_score_brooklyn_v2 - negative_score_brooklyn_v2')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_brooklyn_v2,
        earnings_shadow_confidence_citywide_v2 AS earnings_shadow_confidence_staten_island_v2,
        {clip01(f"{clip01('positive_score_staten_island_v2 - negative_score_staten_island_v2')} * earnings_shadow_confidence_citywide_v2")} AS earnings_shadow_score_staten_island_v2,
        {clip01('0.75 * earnings_shadow_confidence_citywide_v2 + 0.25 * COALESCE(balanced_trip_share_n, 0.0)')} AS earnings_shadow_confidence_manhattan_v3,
        {clip01(f"{clip01('positive_score_manhattan_v3 - negative_score_manhattan_v3')} * {clip01('0.75 * earnings_shadow_confidence_citywide_v2 + 0.25 * COALESCE(balanced_trip_share_n, 0.0)')}")} AS earnings_shadow_score_manhattan_v3,
        {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')} AS earnings_shadow_confidence_bronx_wash_heights_v3,
        {clip01(f"{clip01('positive_score_bronx_wash_heights_v3 - negative_score_bronx_wash_heights_v3')} * {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')}")} AS earnings_shadow_score_bronx_wash_heights_v3,
        {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')} AS earnings_shadow_confidence_queens_v3,
        {clip01(f"{clip01('positive_score_queens_v3 - negative_score_queens_v3')} * {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')}")} AS earnings_shadow_score_queens_v3,
        {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')} AS earnings_shadow_confidence_brooklyn_v3,
        {clip01(f"{clip01('positive_score_brooklyn_v3 - negative_score_brooklyn_v3')} * {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')}")} AS earnings_shadow_score_brooklyn_v3,
        {clip01('0.80 * earnings_shadow_confidence_citywide_v2 + 0.20 * COALESCE(balanced_trip_share_n, 0.0)')} AS earnings_shadow_confidence_staten_island_v3,
        {clip01(f"{clip01('positive_score_staten_island_v3 - negative_score_staten_island_v3')} * {clip01('0.80 * earnings_shadow_confidence_citywide_v2 + 0.20 * COALESCE(balanced_trip_share_n, 0.0)')}")} AS earnings_shadow_score_staten_island_v3,
        {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')} AS earnings_shadow_confidence_citywide_v3,
        {clip01('shadow_score_raw_citywide_v3 * citywide_manhattan_saturation_discount_factor_n')} AS earnings_shadow_score_citywide_v3_anchor_shadow,
        {clip01(f"{clip01('positive_score_citywide_v3 - negative_score_citywide_v3')} * {clip01('0.85 * earnings_shadow_confidence_citywide_v2 + 0.15 * demand_support_n')} * citywide_manhattan_saturation_discount_factor_n")} AS earnings_shadow_score_citywide_v3
      FROM scored
    )
    SELECT
      PULocationID,
      dow_m,
      bin_start_min,
      pickups_now,
      pickups_next,
      median_driver_pay,
      median_pay_per_min,
      median_pay_per_mile,
      median_request_to_pickup_min,
      short_trip_share_3mi_12min,
      shared_ride_share,
      zone_area_sq_miles,
      pickups_per_sq_mile_now,
      pickups_per_sq_mile_next,
      long_trip_share_20plus,
      balanced_trip_share,
      same_zone_dropoff_share,
      downstream_next_value_raw,
      demand_now_n,
      demand_next_n,
      pay_n,
      pay_per_min_n,
      pay_per_mile_n,
      pickup_friction_penalty_n,
      short_trip_penalty_n,
      shared_ride_penalty_n,
      downstream_value_n,
      demand_density_now_n,
      demand_density_next_n,
      demand_support_n AS demand_support_n_shadow,
      density_support_n AS density_support_n_shadow,
      effective_demand_density_now_n AS effective_demand_density_now_n_shadow,
      effective_demand_density_next_n AS effective_demand_density_next_n_shadow,
      busy_now_base_n AS busy_now_base_n_shadow,
      busy_next_base_n AS busy_next_base_n_shadow,
      long_trip_share_20plus_n,
      balanced_trip_share_n AS balanced_trip_share_n_shadow,
      balanced_trip_share AS balanced_trip_share_shadow,
      same_zone_retention_penalty_n,
      churn_pressure_n AS churn_pressure_n_shadow,
      manhattan_core_saturation_proxy_n AS manhattan_core_saturation_proxy_n_shadow,
      manhattan_core_saturation_penalty_n AS manhattan_core_saturation_penalty_n_shadow,
      market_saturation_pressure_n AS market_saturation_pressure_n_shadow,
      market_saturation_penalty_n AS market_saturation_penalty_n_shadow,
      citywide_manhattan_saturation_discount_factor_n AS citywide_manhattan_saturation_discount_factor_shadow,
      positive_score_citywide_v3 AS earnings_shadow_positive_citywide_v3,
      negative_score_citywide_v3 AS earnings_shadow_negative_citywide_v3,
      shadow_score_raw_citywide_v3 AS earnings_shadow_score_raw_citywide_v3,
      shadow_score_raw_citywide_v3 AS earnings_shadow_score_raw_citywide_v3_pre_manhattan_discount_shadow,
      {clip01('shadow_score_raw_citywide_v3 * citywide_manhattan_saturation_discount_factor_n')} AS earnings_shadow_score_citywide_v3_anchor_shadow,
      shadow_score_raw_manhattan_v3 AS earnings_shadow_score_raw_manhattan_v3,
      shadow_score_raw_bronx_wash_heights_v3 AS earnings_shadow_score_raw_bronx_wash_heights_v3,
      shadow_score_raw_queens_v3 AS earnings_shadow_score_raw_queens_v3,
      shadow_score_raw_brooklyn_v3 AS earnings_shadow_score_raw_brooklyn_v3,
      shadow_score_raw_staten_island_v3 AS earnings_shadow_score_raw_staten_island_v3,
      ({c3_busy_now_weight:.8f} * busy_now_base_n + {c3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_citywide_v3,
      ({c3w.pay_weight:.8f} * pay_n + {c3w.pay_per_min_weight:.8f} * pay_per_min_n + {c3w.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_citywide_v3,
      ({c3w.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {c3w.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) +
      CASE
        WHEN {manhattan_core_citywide_guard_sql}
        THEN 0.045 * citywide_manhattan_escape_bonus_n
        ELSE 0.0
      END AS earnings_shadow_trip_mix_positive_citywide_v3,
      ({c3w.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_citywide_v3,
      ({c3w.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_citywide_v3,
      ({c3w.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_citywide_v3,
      ({c3w.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {c3w.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_citywide_v3,
      ({c3w.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n) +
      CASE
        WHEN {manhattan_core_citywide_guard_sql}
        THEN
          0.060 * manhattan_core_saturation_penalty_n +
          0.070 * citywide_manhattan_short_trip_trap_penalty_n
        ELSE 0.0
      END AS earnings_shadow_saturation_penalty_citywide_v3,
      ({mw3_busy_now_weight:.8f} * busy_now_base_n + {mw3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_manhattan_v3,
      ({mw3.pay_weight:.8f} * pay_n + {mw3.pay_per_min_weight:.8f} * pay_per_min_n + {mw3.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_manhattan_v3,
      ({mw3.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {mw3.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) AS earnings_shadow_trip_mix_positive_manhattan_v3,
      ({mw3.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_manhattan_v3,
      ({mw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_manhattan_v3,
      ({mw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_manhattan_v3,
      ({mw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {mw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_manhattan_v3,
      ({mw3.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n + 0.10 * manhattan_core_saturation_penalty_n) AS earnings_shadow_saturation_penalty_manhattan_v3,
      ({bw3_busy_now_weight:.8f} * busy_now_base_n + {bw3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_bronx_wash_heights_v3,
      ({bw3.pay_weight:.8f} * pay_n + {bw3.pay_per_min_weight:.8f} * pay_per_min_n + {bw3.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_bronx_wash_heights_v3,
      ({bw3.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {bw3.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) AS earnings_shadow_trip_mix_positive_bronx_wash_heights_v3,
      ({bw3.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_bronx_wash_heights_v3,
      ({bw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_bronx_wash_heights_v3,
      ({bw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_bronx_wash_heights_v3,
      ({bw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {bw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_bronx_wash_heights_v3,
      ({bw3.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n) AS earnings_shadow_saturation_penalty_bronx_wash_heights_v3,
      ({qw3_busy_now_weight:.8f} * busy_now_base_n + {qw3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_queens_v3,
      ({qw3.pay_weight:.8f} * pay_n + {qw3.pay_per_min_weight:.8f} * pay_per_min_n + {qw3.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_queens_v3,
      ({qw3.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {qw3.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) AS earnings_shadow_trip_mix_positive_queens_v3,
      ({qw3.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_queens_v3,
      ({qw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_queens_v3,
      ({qw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_queens_v3,
      ({qw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {qw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_queens_v3,
      ({qw3.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n) AS earnings_shadow_saturation_penalty_queens_v3,
      ({bkw3_busy_now_weight:.8f} * busy_now_base_n + {bkw3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_brooklyn_v3,
      ({bkw3.pay_weight:.8f} * pay_n + {bkw3.pay_per_min_weight:.8f} * pay_per_min_n + {bkw3.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_brooklyn_v3,
      ({bkw3.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {bkw3.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) AS earnings_shadow_trip_mix_positive_brooklyn_v3,
      ({bkw3.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_brooklyn_v3,
      ({bkw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_brooklyn_v3,
      ({bkw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_brooklyn_v3,
      ({bkw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {bkw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_brooklyn_v3,
      ({bkw3.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n) AS earnings_shadow_saturation_penalty_brooklyn_v3,
      ({sw3_busy_now_weight:.8f} * busy_now_base_n + {sw3_busy_next_weight:.8f} * busy_next_base_n) AS earnings_shadow_busy_size_positive_staten_island_v3,
      ({sw3.pay_weight:.8f} * pay_n + {sw3.pay_per_min_weight:.8f} * pay_per_min_n + {sw3.pay_per_mile_weight:.8f} * pay_per_mile_n) AS earnings_shadow_pay_quality_positive_staten_island_v3,
      ({sw3.balanced_trip_share_weight:.8f} * COALESCE(balanced_trip_share_n, 0.0) + {sw3.long_trip_share_20plus_weight:.8f} * COALESCE(long_trip_share_20plus_n, 0.0)) AS earnings_shadow_trip_mix_positive_staten_island_v3,
      ({sw3.downstream_weight:.8f} * downstream_value_n) AS earnings_shadow_continuation_positive_staten_island_v3,
      ({sw3.short_trip_penalty_weight:.8f} * short_trip_penalty_n) AS earnings_shadow_short_trip_penalty_staten_island_v3,
      ({sw3.same_zone_retention_penalty_weight:.8f} * COALESCE(same_zone_retention_penalty_n, 0.0)) AS earnings_shadow_retention_penalty_staten_island_v3,
      ({sw3.pickup_friction_penalty_weight:.8f} * pickup_friction_penalty_n + {sw3.shared_ride_penalty_weight:.8f} * shared_ride_penalty_n) AS earnings_shadow_friction_penalty_staten_island_v3,
      ({sw3.market_saturation_penalty_weight:.8f} * market_saturation_penalty_n) AS earnings_shadow_saturation_penalty_staten_island_v3,
      earnings_shadow_score_citywide_v3,
      earnings_shadow_confidence_citywide_v3,
      earnings_shadow_confidence_citywide_v3 AS citywide_v3_confidence_profile_shadow,
      earnings_shadow_confidence_manhattan_v3 AS manhattan_v3_confidence_profile_shadow,
      earnings_shadow_confidence_bronx_wash_heights_v3 AS bronx_wash_heights_v3_confidence_profile_shadow,
      earnings_shadow_confidence_queens_v3 AS queens_v3_confidence_profile_shadow,
      earnings_shadow_confidence_brooklyn_v3 AS brooklyn_v3_confidence_profile_shadow,
      earnings_shadow_confidence_staten_island_v3 AS staten_island_v3_confidence_profile_shadow,
      CAST(ROUND(1 + 99 * earnings_shadow_score_citywide_v3) AS INTEGER) AS earnings_shadow_rating_citywide_v3,
      CASE
        WHEN earnings_shadow_rating_citywide_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_citywide_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_citywide_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_citywide_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_citywide_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_citywide_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_citywide_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_citywide_v3,
      CASE
        WHEN earnings_shadow_rating_citywide_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_citywide_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_citywide_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_citywide_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_citywide_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_citywide_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_citywide_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_citywide_v3,
      earnings_shadow_score_citywide_v2,
      earnings_shadow_confidence_citywide_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_citywide_v2) AS INTEGER) AS earnings_shadow_rating_citywide_v2,
      CASE
        WHEN earnings_shadow_rating_citywide_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_citywide_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_citywide_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_citywide_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_citywide_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_citywide_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_citywide_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_citywide_v2,
      CASE
        WHEN earnings_shadow_rating_citywide_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_citywide_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_citywide_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_citywide_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_citywide_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_citywide_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_citywide_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_citywide_v2,
      earnings_shadow_score_manhattan_v2,
      earnings_shadow_confidence_manhattan_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_manhattan_v2) AS INTEGER) AS earnings_shadow_rating_manhattan_v2,
      CASE
        WHEN earnings_shadow_rating_manhattan_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_manhattan_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_manhattan_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_manhattan_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_manhattan_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_manhattan_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_manhattan_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_manhattan_v2,
      CASE
        WHEN earnings_shadow_rating_manhattan_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_manhattan_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_manhattan_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_manhattan_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_manhattan_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_manhattan_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_manhattan_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_manhattan_v2,
      earnings_shadow_score_bronx_wash_heights_v2,
      earnings_shadow_confidence_bronx_wash_heights_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_bronx_wash_heights_v2) AS INTEGER) AS earnings_shadow_rating_bronx_wash_heights_v2,
      CASE
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_bronx_wash_heights_v2,
      CASE
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_bronx_wash_heights_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_bronx_wash_heights_v2,
      earnings_shadow_score_queens_v2,
      earnings_shadow_confidence_queens_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_queens_v2) AS INTEGER) AS earnings_shadow_rating_queens_v2,
      CASE
        WHEN earnings_shadow_rating_queens_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_queens_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_queens_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_queens_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_queens_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_queens_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_queens_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_queens_v2,
      CASE
        WHEN earnings_shadow_rating_queens_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_queens_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_queens_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_queens_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_queens_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_queens_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_queens_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_queens_v2,
      earnings_shadow_score_brooklyn_v2,
      earnings_shadow_confidence_brooklyn_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_brooklyn_v2) AS INTEGER) AS earnings_shadow_rating_brooklyn_v2,
      CASE
        WHEN earnings_shadow_rating_brooklyn_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_brooklyn_v2,
      CASE
        WHEN earnings_shadow_rating_brooklyn_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_brooklyn_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_brooklyn_v2,
      earnings_shadow_score_staten_island_v2,
      earnings_shadow_confidence_staten_island_v2,
      CAST(ROUND(1 + 99 * earnings_shadow_score_staten_island_v2) AS INTEGER) AS earnings_shadow_rating_staten_island_v2,
      CASE
        WHEN earnings_shadow_rating_staten_island_v2 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_staten_island_v2 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_staten_island_v2 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_staten_island_v2 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_staten_island_v2 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_staten_island_v2 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_staten_island_v2 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_staten_island_v2,
      CASE
        WHEN earnings_shadow_rating_staten_island_v2 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_staten_island_v2 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_staten_island_v2 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_staten_island_v2 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_staten_island_v2 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_staten_island_v2 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_staten_island_v2 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_staten_island_v2,
      earnings_shadow_score_manhattan_v3,
      earnings_shadow_confidence_manhattan_v3,
      CAST(ROUND(1 + 99 * earnings_shadow_score_manhattan_v3) AS INTEGER) AS earnings_shadow_rating_manhattan_v3,
      CASE
        WHEN earnings_shadow_rating_manhattan_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_manhattan_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_manhattan_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_manhattan_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_manhattan_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_manhattan_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_manhattan_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_manhattan_v3,
      CASE
        WHEN earnings_shadow_rating_manhattan_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_manhattan_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_manhattan_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_manhattan_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_manhattan_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_manhattan_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_manhattan_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_manhattan_v3,
      earnings_shadow_score_bronx_wash_heights_v3,
      earnings_shadow_confidence_bronx_wash_heights_v3,
      CAST(ROUND(1 + 99 * earnings_shadow_score_bronx_wash_heights_v3) AS INTEGER) AS earnings_shadow_rating_bronx_wash_heights_v3,
      CASE
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_bronx_wash_heights_v3,
      CASE
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_bronx_wash_heights_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_bronx_wash_heights_v3,
      earnings_shadow_score_queens_v3,
      earnings_shadow_confidence_queens_v3,
      CAST(ROUND(1 + 99 * earnings_shadow_score_queens_v3) AS INTEGER) AS earnings_shadow_rating_queens_v3,
      CASE
        WHEN earnings_shadow_rating_queens_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_queens_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_queens_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_queens_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_queens_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_queens_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_queens_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_queens_v3,
      CASE
        WHEN earnings_shadow_rating_queens_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_queens_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_queens_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_queens_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_queens_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_queens_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_queens_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_queens_v3,
      earnings_shadow_score_brooklyn_v3,
      earnings_shadow_confidence_brooklyn_v3,
      CAST(ROUND(1 + 99 * earnings_shadow_score_brooklyn_v3) AS INTEGER) AS earnings_shadow_rating_brooklyn_v3,
      CASE
        WHEN earnings_shadow_rating_brooklyn_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_brooklyn_v3,
      CASE
        WHEN earnings_shadow_rating_brooklyn_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_brooklyn_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_brooklyn_v3,
      earnings_shadow_score_staten_island_v3,
      earnings_shadow_confidence_staten_island_v3,
      CAST(ROUND(1 + 99 * earnings_shadow_score_staten_island_v3) AS INTEGER) AS earnings_shadow_rating_staten_island_v3,
      CASE
        WHEN earnings_shadow_rating_staten_island_v3 >= 87 THEN 'green'
        WHEN earnings_shadow_rating_staten_island_v3 >= 73 THEN 'purple'
        WHEN earnings_shadow_rating_staten_island_v3 >= 60 THEN 'indigo'
        WHEN earnings_shadow_rating_staten_island_v3 >= 48 THEN 'blue'
        WHEN earnings_shadow_rating_staten_island_v3 >= 40 THEN 'sky'
        WHEN earnings_shadow_rating_staten_island_v3 >= 33 THEN 'yellow'
        WHEN earnings_shadow_rating_staten_island_v3 >= 25 THEN 'orange'
        ELSE 'red'
      END AS earnings_shadow_bucket_staten_island_v3,
      CASE
        WHEN earnings_shadow_rating_staten_island_v3 >= 87 THEN '#00b050'
        WHEN earnings_shadow_rating_staten_island_v3 >= 73 THEN '#8000ff'
        WHEN earnings_shadow_rating_staten_island_v3 >= 60 THEN '#4b3cff'
        WHEN earnings_shadow_rating_staten_island_v3 >= 48 THEN '#0066ff'
        WHEN earnings_shadow_rating_staten_island_v3 >= 40 THEN '#66ccff'
        WHEN earnings_shadow_rating_staten_island_v3 >= 33 THEN '#ffd400'
        WHEN earnings_shadow_rating_staten_island_v3 >= 25 THEN '#ff8c00'
        ELSE '#e60000'
      END AS earnings_shadow_color_staten_island_v3
    FROM final
    ORDER BY dow_m, bin_start_min, PULocationID
    """
