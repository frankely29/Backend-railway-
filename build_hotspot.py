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
    CAST(EXTRACT('dow' FROM pickup_datetime) AS INTEGER) AS dow_i,  -- 0=Sun..6=Sat
    CAST(EXTRACT('hour' FROM pickup_datetime) AS INTEGER) AS hour_i,
    CAST(EXTRACT('minute' FROM pickup_datetime) AS INTEGER) AS minute_i,
    driver_pay
  FROM base
),
binned AS (
  SELECT
    PULocationID,
    CASE WHEN dow_i = 0 THEN 6 ELSE dow_i - 1 END AS dow_m,  -- Mon=0..Sun=6
    CAST(FLOOR((hour_i*60 + minute_i) / {int(bin_minutes)}) * {int(bin_minutes)} AS INTEGER) AS bin_start_min,
    driver_pay
  FROM t
),
agg AS (
  SELECT
    PULocationID,
    dow_m,
    bin_start_min,
    COUNT(*) AS pickups,
    AVG(driver_pay) AS avg_driver_pay
  FROM binned
  GROUP BY 1,2,3
  HAVING COUNT(*) >= {int(min_trips_per_window)}
),

-- ✅ Window-relative normalization (but using log to reduce domination)
win AS (
  SELECT
    *,
    LN(1 + pickups) AS log_pickups,
    MIN(LN(1 + pickups)) OVER (PARTITION BY dow_m, bin_start_min) AS min_log_pickups,
    MAX(LN(1 + pickups)) OVER (PARTITION BY dow_m, bin_start_min) AS max_log_pickups,
    MIN(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS min_pay,
    MAX(avg_driver_pay) OVER (PARTITION BY dow_m, bin_start_min) AS max_pay
  FROM agg
),
win_scored AS (
  SELECT
    PULocationID, dow_m, bin_start_min, pickups, avg_driver_pay,

    CASE
      WHEN max_log_pickups IS NULL OR min_log_pickups IS NULL OR max_log_pickups = min_log_pickups THEN 0.0
      ELSE (log_pickups - min_log_pickups) * 1.0 / (max_log_pickups - min_log_pickups)
    END AS vol_n,

    CASE
      WHEN max_pay IS NULL OR min_pay IS NULL OR max_pay = min_pay THEN 0.0
      ELSE (avg_driver_pay - min_pay) * 1.0 / (max_pay - min_pay)
    END AS pay_n
  FROM win
),

-- ✅ Zone baseline across ALL windows (absolute “this zone can be good” signal)
zone_base AS (
  SELECT
    PULocationID,
    LN(1 + AVG(pickups)) AS base_log_pickups,
    AVG(avg_driver_pay) AS base_pay
  FROM agg
  GROUP BY 1
),
zone_mm AS (
  SELECT
    *,
    MIN(base_log_pickups) OVER () AS min_base_log_pickups,
    MAX(base_log_pickups) OVER () AS max_base_log_pickups,
    MIN(base_pay) OVER () AS min_base_pay,
    MAX(base_pay) OVER () AS max_base_pay
  FROM zone_base
),
zone_norm AS (
  SELECT
    PULocationID,
    CASE
      WHEN max_base_log_pickups IS NULL OR min_base_log_pickups IS NULL OR max_base_log_pickups = min_base_log_pickups THEN 0.0
      ELSE (base_log_pickups - min_base_log_pickups) * 1.0 / (max_base_log_pickups - min_base_log_pickups)
    END AS base_vol_n,
    CASE
      WHEN max_base_pay IS NULL OR min_base_pay IS NULL OR max_base_pay = min_base_pay THEN 0.0
      ELSE (base_pay - min_base_pay) * 1.0 / (max_base_pay - min_base_pay)
    END AS base_pay_n
  FROM zone_mm
),

final AS (
  SELECT
    w.PULocationID,
    w.dow_m,
    w.bin_start_min,
    w.pickups,
    w.avg_driver_pay,

    -- Moment score (relative THIS window)
    (0.80*w.vol_n + 0.20*w.pay_n) AS moment_score,

    -- Baseline score (absolute over ALL windows)
    (0.80*z.base_vol_n + 0.20*z.base_pay_n) AS base_score,

    -- Confidence: saturates at ~50 trips; low sample = less aggressive “good moment”
    LEAST(1.0, w.pickups / 50.0) AS conf
  FROM win_scored w
  JOIN zone_norm z USING (PULocationID)
)

SELECT
  PULocationID,
  dow_m,
  bin_start_min,
  pickups,
  avg_driver_pay,

  CAST(
    ROUND(
      1 + 99 * LEAST(
        GREATEST(
          -- ✅ Composite score (data-driven realism)
          (
            (0.70*moment_score + 0.30*base_score)      -- mostly moment, anchored by baseline
            * (0.50 + 0.50*conf)                       -- confidence scales effect (0.5..1.0)
          ),
          0.0
        ),
        1.0
      )
    ) AS INTEGER
  ) AS rating
FROM final
ORDER BY dow_m, bin_start_min, PULocationID;
"""