{{ config(
    materialized = 'table'
) }}

WITH monthly AS (

    SELECT
        city_id,
        city_name,
        country_code,
        year,
        month,
        avg_tmean_c,
        avg_tmax_c,
        avg_tmin_c,
        total_precip_mm
    FROM {{ ref('clean_monthly_climate') }}

),

clim AS (

    SELECT
        city_id,
        month,
        climatology_tmean_c,
        climatology_tmax_c,
        climatology_tmin_c,
        climatology_total_precip_mm,
        climatology_tmean_std_c,
        climatology_total_precip_std_mm
    FROM {{ ref('climatology_city_month') }}

),

joined AS (

    SELECT
        m.city_id,
        m.city_name,
        m.country_code,
        m.year,
        m.month,

        -- Helpful labels
        CASE m.month
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,

        CASE
            WHEN m.month IN (12, 1, 2) THEN 'DJF'  -- Northern Hemisphere winter
            WHEN m.month IN (3, 4, 5)  THEN 'MAM'
            WHEN m.month IN (6, 7, 8)  THEN 'JJA'
            WHEN m.month IN (9, 10, 11) THEN 'SON'
        END AS season,

        -- Actuals
        m.avg_tmean_c,
        m.avg_tmax_c,
        m.avg_tmin_c,
        m.total_precip_mm,

        -- Climatology (baseline)
        c.climatology_tmean_c,
        c.climatology_tmax_c,
        c.climatology_tmin_c,
        c.climatology_total_precip_mm,
        c.climatology_tmean_std_c,
        c.climatology_total_precip_std_mm,

        -- Temperature anomalies (actual - climatology)
        (m.avg_tmean_c - c.climatology_tmean_c) AS anomaly_tmean_c,
        (m.avg_tmax_c  - c.climatology_tmax_c)  AS anomaly_tmax_c,
        (m.avg_tmin_c  - c.climatology_tmin_c)  AS anomaly_tmin_c,

        -- Precipitation anomaly
        (m.total_precip_mm - c.climatology_total_precip_mm) AS anomaly_total_precip_mm,

        -- Standardized anomalies (z-scores) - guard against zero/NULL stddev
        CASE
            WHEN c.climatology_tmean_std_c IS NOT NULL
                 AND c.climatology_tmean_std_c > 0
            THEN (m.avg_tmean_c - c.climatology_tmean_c) / c.climatology_tmean_std_c
        END AS zscore_tmean,

        CASE
            WHEN c.climatology_total_precip_std_mm IS NOT NULL
                 AND c.climatology_total_precip_std_mm > 0
            THEN (m.total_precip_mm - c.climatology_total_precip_mm) / c.climatology_total_precip_std_mm
        END AS zscore_total_precip,

        -- Simple anomaly-event flags (tweak thresholds as needed)
        CASE
            WHEN (m.avg_tmean_c - c.climatology_tmean_c) >= 1.0 THEN TRUE
            ELSE FALSE
        END AS is_positive_temp_anomaly,

        CASE
            WHEN (m.avg_tmean_c - c.climatology_tmean_c) <= -1.0 THEN TRUE
            ELSE FALSE
        END AS is_negative_temp_anomaly,

        CASE
            WHEN
                c.climatology_tmean_std_c IS NOT NULL
                AND c.climatology_tmean_std_c > 0
                AND (m.avg_tmean_c - c.climatology_tmean_c) / c.climatology_tmean_std_c >= 2.0
            THEN TRUE
            ELSE FALSE
        END AS is_strong_positive_temp_anomaly,

        CASE
            WHEN
                c.climatology_tmean_std_c IS NOT NULL
                AND c.climatology_tmean_std_c > 0
                AND (m.avg_tmean_c - c.climatology_tmean_c) / c.climatology_tmean_std_c <= -2.0
            THEN TRUE
            ELSE FALSE
        END AS is_strong_negative_temp_anomaly

    FROM monthly m
    LEFT JOIN clim c
        ON m.city_id = c.city_id
       AND m.month   = c.month

)

SELECT *
FROM joined
ORDER BY city_name, year, month