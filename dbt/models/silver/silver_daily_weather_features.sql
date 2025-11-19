{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT *
    FROM {{ ref('bronze_daily_weather') }}
),

cleaned AS (
    SELECT
        city_id,
        city_name,
        country_code,
        date,

        -- Cleaned and renamed temperature fields
        temperature_2m_max        AS tmax_c,
        temperature_2m_min        AS tmin_c,
        temperature_2m_mean       AS tmean_c,
        dew_point_2m_mean         AS dewpoint_c,

        precipitation_sum         AS precip_mm,
        wind_speed_10m_max        AS wind_max_ms,
        shortwave_radiation_sum   AS sw_radiation,

        -- Calendar features
        EXTRACT(year  FROM date)       AS year,
        EXTRACT(month FROM date)       AS month,
        EXTRACT(doy   FROM date)       AS day_of_year,
        CASE WHEN EXTRACT(month FROM date) IN (6,7,8) THEN 1 ELSE 0 END AS is_summer,

        -- Comfort/extreme flags
        CASE WHEN temperature_2m_max >= 30 THEN 1 ELSE 0 END AS is_heat_day,
        CASE WHEN temperature_2m_min >= 20 THEN 1 ELSE 0 END AS is_tropical_night,
        CASE WHEN precipitation_sum >= 10 THEN 1 ELSE 0 END AS is_heavy_precip_day

    FROM base
)

SELECT *
FROM cleaned