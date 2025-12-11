{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('clean_daily_weather_features') }}
),

aggregated AS (
    SELECT
        city_id,
        city_name,
        country_code,
        year,
        month,

        COUNT(*) AS days_in_month,

        -- Monthly averages
        AVG(tmax_c)      AS avg_tmax_c,
        AVG(tmin_c)      AS avg_tmin_c,
        AVG(tmean_c)     AS avg_tmean_c,
        AVG(dewpoint_c)  AS avg_dewpoint_c,
        AVG(wind_max_ms) AS avg_wind_max_ms,
        AVG(sw_radiation) AS avg_sw_radiation,

        -- Precipitation totals
        SUM(precip_mm)   AS total_precip_mm,

        -- Counts of events
        SUM(is_heat_day)         AS heat_day_count,
        SUM(is_tropical_night)   AS tropical_night_count,
        SUM(is_heavy_precip_day) AS heavy_precip_day_count,
        SUM(is_summer)           AS summer_day_count

    FROM base
    GROUP BY
        city_id,
        city_name,
        country_code,
        year,
        month
)

SELECT *
FROM aggregated