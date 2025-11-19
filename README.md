# California–Portugal Climate Pipeline
### A Data Engineering & Machine Learning Project Using Open-Meteo APIs

This project builds a full data engineering, analytics, and machine learning pipeline to compare the climates of four coastal Mediterranean-like cities:

- Los Angeles (US)
- San Francisco (US)
- Lisbon (Portugal)
- Porto (Portugal)

Using daily historical weather data from 1980-01-01 to 2024-12-31, the pipeline analyzes temperature cycles, dew-point trends, precipitation patterns, solar radiation, coastal climate similarities, and long-term climate variability. It also prepares ML-ready features for forecasting tasks such as next-day maximum temperature.

The project integrates:

- Open-Meteo Geocoding API  
- Open-Meteo Historical Weather API  
- DuckDB (analytical warehouse)  
- dbt + dbt-duckdb (Bronze → Silver → Gold transformations)  
- uv (Python environment and project management)  
- YAML-based configuration system  

The project follows a modern medallion architecture (bronze/silver/gold) used in contemporary data engineering workflows.

---

## 0. Requirements

- Python 3.10+  
- uv (for environment and dependency management)  
- Internet access (for Open-Meteo API calls)

Install dependencies and build the local package:

    uv sync

---

## 1. Pipeline Overview

High-level flow:

    Geocoding API → dim_city seed (CSV)
    Historical Weather API → raw JSON → Bronze (dbt) → Silver/Gold (future)
    Bronze → analytics, ML features, and visualizations (future)

The Bronze layer is currently implemented as a DuckDB table:

- main.dim_city (dbt seed)
- main.bronze_daily_weather (dbt model)

---

## 2. Data Specification

### 2.1 Cities

The pipeline includes four target cities:

| city_id | city_name     | country_code |
|--------:|---------------|--------------|
| 1       | Los Angeles   | US           |
| 2       | San Francisco | US           |
| 3       | Lisbon        | PT           |
| 4       | Porto         | PT           |

City coordinates and metadata (latitude, longitude, timezone, region) are retrieved dynamically from the Open-Meteo Geocoding API and stored in:

- data/raw/geocoding/*.json (raw API responses)
- dbt/seeds/dim_city.csv (seed table for dbt)

---

### 2.2 Daily Variables (Historical Weather API)

The following daily variables are ingested for each city:

- temperature_2m_max  
- temperature_2m_min  
- temperature_2m_mean  
- dew_point_2m_mean  
- precipitation_sum  
- wind_speed_10m_max  
- shortwave_radiation_sum  

These variables support climate comparison, humidity/comfort analysis, precipitation regimes, solar radiation patterns, and ML feature engineering.

---

### 2.3 Time Window

The ingestion window is:

    1980-01-01 → 2024-12-31

This period provides enough temporal depth to analyze both weather variability and long-term climate tendencies.

---

### 2.4 Batching Strategy

API requests are executed in city–year batches to remain polite to the API and allow checkpointing.

Raw responses are stored as:

    data/raw/open_meteo_daily/<city_slug>/<year>.json

Examples:

- data/raw/open_meteo_daily/los_angeles/1980.json  
- data/raw/open_meteo_daily/san_francisco/1995.json  

---

### 2.5 Raw Data Format

All raw historical responses are stored as JSON, exactly as returned by the API.

No renaming or transformation occurs in the ingestion scripts. dbt handles flattening and normalization in the Bronze layer.

---

## 3. Warehouse Schema & Transformation Strategy

### 3.1 Bronze Layer: `bronze_daily_weather`

The Bronze layer stores flattened daily weather records, one row per:

- city_id  
- date  

Columns (DuckDB table main.bronze_daily_weather):

- city_id (INTEGER)  
- city_name (TEXT)  
- country_code (TEXT)  
- city_slug (TEXT)  
- year (INTEGER)  
- date (DATE)  
- temperature_2m_max (DOUBLE)  
- temperature_2m_min (DOUBLE)  
- temperature_2m_mean (DOUBLE)  
- dew_point_2m_mean (DOUBLE)  
- precipitation_sum (DOUBLE)  
- wind_speed_10m_max (DOUBLE)  
- shortwave_radiation_sum (DOUBLE)  

Implementation notes:

- dbt model: dbt/models/bronze/bronze_daily_weather.sql  
- Uses DuckDB’s read_json_auto to read all JSON files  
- Extracts city_slug and year from the filename  
- Uses array indexing to explode daily arrays into one row per date  
- Joins to dim_city via a slugified city_name to attach metadata  

---

### 3.2 Transformation Strategy

- Python ingestion scripts only download JSON and write to disk.  
- dbt + DuckDB handle:
  - Flattening the JSON into rows  
  - Joining to dim_city  
  - Building Bronze tables suitable for downstream analytics  

Future work (Silver/Gold):

- Silver: cleaned daily tables with flags (heat days, tropical nights, etc.)  
- Gold: monthly/seasonal climatologies for California vs Portugal storytelling, plus ML-ready feature tables.

---

## 4. Configuration & Tooling

### 4.1 Environment (uv)

The Python environment is managed via uv, which provides:

- Reproducible environments  
- Fast installs  
- Project-local .venv

To install dependencies:

    uv sync

To run a CLI entrypoint:

    uv run <command_name>

---

### 4.2 YAML Configuration

Project settings and metadata live in:

- src/config/settings.yaml  
- src/config/cities.yaml  

They specify:

- API base URLs and daily variable list  
- Data directories  
- Time window (start_date, end_date)  
- City IDs, names, and country codes  

---

### 4.3 Logging

Ingestion scripts log to:

- logs/ingestion.log  

Logs include:

- Geocoding requests and results  
- Historical weather API calls  
- File write locations  
- Retry/backoff messages if rate-limited

---

## 5. Directory Structure

Top-level structure (simplified):

    california-portugal-climate/
    ├── README.md
    ├── pyproject.toml
    ├── uv.lock
    ├── .gitignore
    ├── data/
    │   ├── raw/
    │   │   ├── geocoding/
    │   │   └── open_meteo_daily/
    │   └── warehouse/
    │       └── climate.duckdb
    ├── dbt/
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   ├── models/
    │   │   └── bronze/
    │   │       └── bronze_daily_weather.sql
    │   ├── seeds/
    │   │   └── dim_city.csv
    │   ├── macros/
    │   ├── analyses/
    │   ├── snapshots/
    │   └── tests/
    ├── src/
    │   ├── climate_pipeline/
    │   │   ├── ingestion/
    │   │   │   ├── geocoding.py
    │   │   │   └── fetch_daily_weather.py
    │   │   ├── utils/
    │   │   │   └── open_meteo_client.py
    │   │   ├── pipelines/
    │   │   ├── features/
    │   │   └── ml/
    │   └── config/
    │       ├── settings.yaml
    │       └── cities.yaml
    ├── dashboards/
    ├── orchestration/
    ├── logs/
    └── tests/

---

## 6. Running the Pipeline

### 6.1 Step 1 — Geocode cities

From the project root:

    uv run geocode-cities

This:

- Calls the Open-Meteo Geocoding API for each city in src/config/cities.yaml  
- Writes raw geocoding JSON to data/raw/geocoding/  
- Writes dbt/seeds/dim_city.csv  

---

### 6.2 Step 2 — Fetch daily historical weather

From the project root:

    uv run fetch-daily-weather

This:

- Reads dim_city.csv and src/config/settings.yaml  
- Loops over all configured years (1980–2024) for each city  
- Obtains daily historical weather via the Open-Meteo Archive API  
- Writes JSON to data/raw/open_meteo_daily/<city_slug>/<year>.json  
- Is idempotent: existing files are skipped  

The OpenMeteoClient includes a simple retry with exponential backoff for HTTP 429 (Too Many Requests).

---

### 6.3 Step 3 — Build the warehouse (dbt + DuckDB)

From the project root, first ensure the DuckDB directory exists:

    mkdir -p data/warehouse

Then from the dbt/ directory:

    cd dbt
    export DBT_PROFILES_DIR=.
    uv run dbt seed
    uv run dbt run --select bronze_daily_weather

This:

- Seeds dim_city into the DuckDB database at data/warehouse/climate.duckdb  
- Builds main.bronze_daily_weather from the raw JSON files  

You can inspect the database with any DuckDB client, or via DuckDB’s CLI.

---

## 7. Future Work

Planned extensions:

- Silver-layer models:
  - Daily tables with additional flags (heat days, tropical nights, etc.)
  - Monthly and seasonal climatologies per city  
- Gold-layer models:
  - Long-term climate profiles comparing California vs Portugal  
  - Feature tables for ML models (e.g., temperature forecasting)  
- Streamlit dashboards and visualizations  
- Optional sea surface temperature (SST) integration from public NOAA/NASA APIs.  

---

## 8. Attribution

This project uses:

- Open-Meteo Geocoding API  
- Open-Meteo Historical Weather API  

Data © Open-Meteo (CC-BY-4.0).