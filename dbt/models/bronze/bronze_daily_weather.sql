{{ config(
    materialized = 'incremental',
    unique_key   = ['city_id', 'date']
) }}

{# ---------------------------------------------
  Path to raw JSON files
  --------------------------------------------- #}

{# Use CLIMATE_DATA_ROOT from env, defaulting to "data" if unset #}
{% set data_root = env_var('CLIMATE_DATA_ROOT', 'data') %}
{% set raw_weather_dir = data_root ~ '/raw/open_meteo_daily' %}

-- Bronze layer: Flatten Open-Meteo daily JSON into one row per city/date.

WITH raw_json AS (

    -- Read all JSON files for all cities/years.
    -- DuckDB's JSON reader adds a `filename` virtual column by default.
    SELECT
        filename,
        daily.time                    AS time_list,
        daily.temperature_2m_max      AS tmax_list,
        daily.temperature_2m_min      AS tmin_list,
        daily.temperature_2m_mean     AS tmean_list,
        daily.dew_point_2m_mean       AS dew_list,
        daily.precipitation_sum       AS precip_list,
        daily.wind_speed_10m_max      AS wind_list,
        daily.shortwave_radiation_sum AS sw_list
    FROM read_json_auto('{{ raw_weather_dir }}/*/*.json')

),

exploded AS (

    -- Explode the daily arrays into one row per day using an index over the array length.
    SELECT
        r.filename,
        r.city_slug,
        EXTRACT(year FROM r.time_list[i]::DATE)   AS year,
        r.time_list[i]::DATE                      AS date,
        r.tmax_list[i]                            AS temperature_2m_max,
        r.tmin_list[i]                            AS temperature_2m_min,
        r.tmean_list[i]                           AS temperature_2m_mean,
        r.dew_list[i]                             AS dew_point_2m_mean,
        r.precip_list[i]                          AS precipitation_sum,
        r.wind_list[i]                            AS wind_speed_10m_max,
        r.sw_list[i]                              AS shortwave_radiation_sum
    FROM (
        SELECT
            filename,

            -- last directory name before the file → city slug (los_angeles, lisbon, ...)
            str_split(filename, '/')[-2] AS city_slug,
            -- last path component (e.g. "1980.json")
            str_split(filename, '/')[-1] AS filename_leaf,

            time_list,
            tmax_list,
            tmin_list,
            tmean_list,
            dew_list,
            precip_list,
            wind_list,
            sw_list,
            array_length(time_list) AS n_days
        FROM raw_json
    ) r,
    UNNEST(range(1, r.n_days + 1)) AS t(i)
),

joined AS (

    -- Join to dim_city on city_slug to get city metadata (id, name, country).
    SELECT
        c.city_id,
        c.city_name,
        c.country_code,
        e.city_slug,
        e.year,
        e.date,
        e.temperature_2m_max,
        e.temperature_2m_min,
        e.temperature_2m_mean,
        e.dew_point_2m_mean,
        e.precipitation_sum,
        e.wind_speed_10m_max,
        e.shortwave_radiation_sum
    FROM exploded e
    LEFT JOIN {{ ref('dim_city') }} c
        ON lower(replace(c.city_name, ' ', '_')) = e.city_slug
)

SELECT *
FROM joined
{% if is_incremental() %}
WHERE date > (SELECT max(date) FROM {{ this }})
{% endif %}