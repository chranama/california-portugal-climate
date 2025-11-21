{{ config(materialized = 'table') }}

WITH anomalies AS (

    SELECT
        city_id,
        city_name,
        year,
        month,
        anomaly_tmean_c
    FROM {{ ref('gold_city_month_anomalies') }}

),

paired AS (

    SELECT
        a.city_id   AS city_id_1,
        a.city_name AS city_name_1,
        b.city_id   AS city_id_2,
        b.city_name AS city_name_2,
        a.anomaly_tmean_c AS anomaly_1,
        b.anomaly_tmean_c AS anomaly_2
    FROM anomalies a
    JOIN anomalies b
        ON a.year = b.year
       AND a.month = b.month
       AND a.city_id < b.city_id   -- avoid duplicates and self-joins

),

correlations AS (

    SELECT
        city_id_1,
        city_name_1,
        city_id_2,
        city_name_2,
        COUNT(*) AS n_observations,
        CORR(anomaly_1, anomaly_2) AS anomaly_correlation
    FROM paired
    GROUP BY
        city_id_1,
        city_name_1,
        city_id_2,
        city_name_2

)

SELECT *
FROM correlations
ORDER BY anomaly_correlation DESC