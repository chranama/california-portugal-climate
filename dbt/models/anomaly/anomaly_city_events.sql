{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        city_id,
        city_name,
        year,
        month,
        anomaly_tmean_c AS temperature_anomaly,
        zscore_tmean    AS z_score
    FROM {{ ref('anomaly_city_month') }}
)

SELECT
    city_id,
    city_name,
    year,
    month,
    temperature_anomaly,
    z_score,

    CASE WHEN z_score >=  2 THEN TRUE ELSE FALSE END AS is_hot_event,
    CASE WHEN z_score <= -2 THEN TRUE ELSE FALSE END AS is_cold_event,
    CASE WHEN ABS(z_score) >= 3 THEN TRUE ELSE FALSE END AS is_extreme_event
FROM base