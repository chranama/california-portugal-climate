# California–Portugal Climate Pipeline  
### A Full-Stack Data Engineering, Analytics & Machine Learning System

This project implements an end-to-end climate data platform comparing four coastal Mediterranean-like cities:

- Los Angeles (US)  
- San Francisco (US)  
- Lisbon (Portugal)  
- Porto (Portugal)

Using daily historical weather data from 1980–2024, the system ingests, transforms, models, and analyzes long-term climate patterns and anomalies, and produces:

- A production-ready DuckDB warehouse  
- A multi-layer dbt pipeline (bronze → silver → gold)  
- An anomaly detection framework  
- An ML-ready feature store  
- A baseline climate anomaly model  
- A Streamlit analytics dashboard  
- An orchestration CLI  
- Automated test coverage  

This architecture is inspired by **modern data engineering standards (medallion architecture)** used in production systems.

---

## Architecture Overview

Open-Meteo APIs → Raw JSON → DuckDB  
                      ↓  
                   dbt (Bronze → Silver → Gold)  
                      ↓  
  Anomaly / ML Features / Correlations  
                      ↓  
    ML Models + Streamlit Dashboard  
                      ↓  
  Single-Command Orchestration + Tests  

---

## Tech Stack

- Open-Meteo APIs (Geocoding + Historical Weather)  
- DuckDB (analytical warehouse)  
- dbt + dbt-duckdb (ELT / transformation)  
- uv (Python env + package runner)  
- scikit-learn (baseline ML modeling)  
- Streamlit + Plotly (dashboard)  
- pytest (testing)  
- YAML-based config system  
- CLI orchestration layer  

---

## Data Overview

### Cities

| city_id | city_name | country_code |
|--------:|----------|--------------|
| 1 | Los Angeles | US |
| 2 | San Francisco | US |
| 3 | Lisbon | PT |
| 4 | Porto | PT |

Coordinates, timezones, and metadata are obtained via the Open-Meteo Geocoding API and stored in:

- data/raw/geocoding/  
- dbt/seeds/dim_city.csv  

### Daily Variables

From Open-Meteo Historical API:

- temperature_2m_max  
- temperature_2m_min  
- temperature_2m_mean  
- dew_point_2m_mean  
- precipitation_sum  
- wind_speed_10m_max  
- shortwave_radiation_sum  

Time window:

    1980-01-01 → 2024-12-31

All raw data is stored as JSON and preserved unchanged.

---

## Warehouse Models (dbt)

### Bronze Layer – `main.bronze_daily_weather`

One row per:

- city  
- date  

Flattened from raw JSON using:

- read_json_auto  
- Array unnesting  
- Join to dim_city  

---

### Silver Layer

**main.silver_daily_weather_features**

Adds:

- heat day flags  
- tropical night flags  
- heavy precipitation day flags  
- seasonal indicators  

**main.silver_monthly_climate**

Monthly aggregation:

- avg temperature  
- total precipitation  
- heat/tropical night counts  
- radiation + wind summaries  

---

### Gold Layer

**main.climatology_city_month**  
Canonical monthly climate baseline per city.

**main.gold_city_month_anomalies**

Includes:

- anomaly_tmean_c  
- zscore_tmean_c  
- is_positive_temp_anomaly  
- is_negative_temp_anomaly  
- is_strong_positive_temp_anomaly  
- is_strong_negative_temp_anomaly  

**main.gold_city_anomaly_lags**

- 1–3 month deltas  
- Rolling means  

**main.gold_city_anomaly_correlations**

- Cross-city correlation values  

**main.gold_city_anomaly_events**

- Binary anomaly classification  

**main.gold_ml_features**

- anomaly values  
- lag values  
- rolling metrics  
- classification flags  

Used for ML training.

---

## Machine Learning Layer

Baseline model:

- RandomForestClassifier  
- Target: is_positive_temp_anomaly  
- Time-based split  
- Imbalanced realistic climatology

Outputs:

- models/baseline_rf.pkl  
- models/baseline_rf_metrics.json  

Run:

    uv run climate-train-baseline

---

## Streamlit Dashboard

Location:

    dashboards/streamlit/app.py

Run:

    uv run streamlit run dashboards/streamlit/app.py

Includes:

- City selector  
- Metric selector  
- Monthly trends  
- Climate comparison  
- Anomaly visualization  

Uses DuckDB as backend.

---

## Orchestration

Run everything:

    uv run climate-run-all

Run with tests:

    uv run climate-run-all --with-tests

This performs:

1. DuckDB lock check  
2. dbt models (all layers)  
3. ML training  
4. pytest (optional)  

This is a **production-style orchestration pipeline**.

---

## Testing

Run:

    uv run pytest

Checks:

- Gold schema correctness  
- ML feature completeness  
- No NaN leakage  
- Table integrity  

Located in:

    tests/

---

## Project Structure (Simplified)

    california-portugal-climate/
        README.md
        pyproject.toml
        uv.lock
        data/
            raw/
            warehouse/
                climate.duckdb
        dbt/
            models/
            seeds/
            macros/
        src/
            climate_pipeline/
                ingestion/
                ml/
                orchestration/
            config/
        dashboards/
        tests/
        logs/

---

## Quick Start

    uv sync
    uv run climate-run-all --with-tests
    uv run streamlit run dashboards/streamlit/app.py

---

## What This Project Demonstrates

- Modern data stack usage  
- End-to-end pipelines  
- Feature engineering for climate  
- Anomaly detection  
- Cross-region climate analysis  
- Model training & monitoring  
- Dashboard delivery  
- Orchestration and CI readiness  

This is a **complete, production-minded data system** — far beyond a simple weather dashboard.

---

## Planned Enhancements

- Transformer / LSTM temporal models  
- Extreme event classifiers  
- Correlation heatmaps  
- SST (sea surface temperature) integration  
- Docker + cloud deployment  
- Public dashboard hosting 