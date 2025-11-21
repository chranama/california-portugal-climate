{{ config(
    materialized = 'table'
) }}

-- Long-term monthly climatology for each city.
-- Baseline period: 1981–2010 (adjust if you prefer).

SELECT
    city_id,
    city_name,
    country_code,
    month,
    AVG(avg_tmean_c) AS climatology_tmean_c,
    AVG(avg_tmax_c)  AS climatology_tmax_c,
    AVG(avg_tmin_c)  AS climatology_tmin_c,
    AVG(total_precip_mm) AS climatology_total_precip_mm,

    -- Standard deviations for standardized anomalies (z-scores)
    STDDEV(avg_tmean_c)        AS climatology_tmean_std_c,
    STDDEV(total_precip_mm)    AS climatology_total_precip_std_mm
FROM {{ ref('silver_monthly_climate') }}
WHERE year BETWEEN 1981 AND 2010
GROUP BY
    city_id,
    city_name,
    country_code,
    month