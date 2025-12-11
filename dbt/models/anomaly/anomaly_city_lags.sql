{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        city_id,
        city_name,
        year,
        month,
        anomaly_tmean_c AS temperature_anomaly
    FROM {{ ref('anomaly_city_month') }}
),

pairs AS (
    SELECT
        a.city_id         AS city_id,
        a.city_name       AS city_name,
        b.city_id         AS other_city_id,
        b.city_name       AS other_city_name,
        a.year            AS year,
        a.month           AS month,
        b.year            AS other_year,
        b.month           AS other_month,
        a.temperature_anomaly AS anomaly,
        b.temperature_anomaly AS other_anomaly
    FROM base a
    JOIN base b
        ON a.city_id != b.city_id
),

lagged AS (
    SELECT
        city_id,
        city_name,
        other_city_id,
        other_city_name,

        ((other_year - year) * 12 + (other_month - month)) AS lag_months,

        anomaly,
        other_anomaly
    FROM pairs
    WHERE ABS((other_year - year) * 12 + (other_month - month)) <= 6
),

aggregated AS (
    SELECT
        city_id,
        city_name,
        other_city_id,
        other_city_name,
        lag_months,

        COUNT(*) AS n_observations,
        CORR(anomaly, other_anomaly) AS anomaly_correlation
    FROM lagged
    GROUP BY
        city_id,
        city_name,
        other_city_id,
        other_city_name,
        lag_months
)

SELECT *
FROM aggregated
ORDER BY
    city_name,
    other_city_name,
    lag_months