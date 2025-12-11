{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        city_id,
        city_name,
        year,
        month,
        anomaly_tmean_c,
        zscore_tmean,
        is_positive_temp_anomaly,
        is_negative_temp_anomaly,
        is_strong_positive_temp_anomaly,
        is_strong_negative_temp_anomaly
    FROM {{ ref('anomaly_city_month') }}
),

-- 1. Create ordered time index
indexed AS (
    SELECT
        *,
        (year * 12 + month) AS time_index
    FROM base
),

-- 2. Rolling statistics & raw lags
rolling AS (
    SELECT
        *,
        AVG(anomaly_tmean_c) OVER (
            PARTITION BY city_id
            ORDER BY time_index
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS roll_mean_3,

        AVG(anomaly_tmean_c) OVER (
            PARTITION BY city_id
            ORDER BY time_index
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS roll_mean_6,

        STDDEV(anomaly_tmean_c) OVER (
            PARTITION BY city_id
            ORDER BY time_index
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS roll_std_3,

        STDDEV(anomaly_tmean_c) OVER (
            PARTITION BY city_id
            ORDER BY time_index
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS roll_std_6,

        LAG(anomaly_tmean_c, 1) OVER (
            PARTITION BY city_id
            ORDER BY time_index
        ) AS lag_1m,

        LAG(anomaly_tmean_c, 3) OVER (
            PARTITION BY city_id
            ORDER BY time_index
        ) AS lag_3m
    FROM indexed
),

-- 3. Momentum deltas
deltas AS (
    SELECT
        *,
        anomaly_tmean_c - lag_1m AS delta_1m,
        anomaly_tmean_c - lag_3m AS delta_3m
    FROM rolling
),

-- 4. Teleconnection summary (best lagged correlation per city)
connections AS (
    SELECT
        city_id,
        MAX(ABS(anomaly_correlation)) AS max_lagged_corr,
        ARG_MAX(lag_months, ABS(anomaly_correlation)) AS lead_lag_months
    FROM {{ ref('anomaly_city_lags') }}
    GROUP BY city_id
),

-- 5. Label: strong anomaly event in the *next* month
next_month AS (
    SELECT
        a.*,
        c.max_lagged_corr,
        c.lead_lag_months,
        CASE
            WHEN b.is_strong_positive_temp_anomaly = TRUE
              OR b.is_strong_negative_temp_anomaly = TRUE
            THEN 1 ELSE 0
        END AS is_event_next_month
    FROM deltas a
    LEFT JOIN deltas b
      ON a.city_id = b.city_id
     AND b.time_index = a.time_index + 1
    LEFT JOIN connections c
      ON a.city_id = c.city_id
),

-- 6. Seasonal encoding
final AS (
    SELECT
        *,
        SIN(2 * PI() * month / 12) AS sin_month,
        COS(2 * PI() * month / 12) AS cos_month
    FROM next_month
)

SELECT
    city_id,
    city_name,
    year,
    month,
    sin_month,
    cos_month,

    anomaly_tmean_c,
    roll_mean_3,
    roll_mean_6,
    roll_std_3,
    roll_std_6,
    delta_1m,
    delta_3m,

    max_lagged_corr,
    lead_lag_months,

    is_event_next_month
FROM final
WHERE roll_mean_6 IS NOT NULL