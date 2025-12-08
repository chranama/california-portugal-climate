# California–Portugal Climate Pipeline  
### A Full-Stack Data Engineering, Analytics & Machine Learning System

This project implements an end-to-end climate data platform comparing four coastal Mediterranean-like cities:

- Los Angeles (US)  
- San Francisco (US)  
- Lisbon (Portugal)  
- Porto (Portugal)

Using daily historical weather data from **1980–2024**, the system ingests, transforms, models, and analyzes long-term climate patterns and anomalies, producing:

- A production-ready DuckDB warehouse  
- A multi-layer dbt pipeline (bronze → silver → gold)  
- An anomaly detection and ML-ready feature store  
- A baseline RandomForest anomaly model  
- A Streamlit analytics dashboard  
- Prefect orchestration (daily + backfill workflows)  
- Automated data quality tests  

This architecture follows **modern medallion design patterns** used in production data systems.

---

## Architecture Overview

    Open-Meteo APIs → Raw JSON → DuckDB
                          ↓
                   dbt (Bronze → Silver → Gold)
                          ↓
            Feature Engineering + Anomaly Detection
                          ↓
            ML Models + Streamlit Analytics Dashboard
                          ↓
               Prefect Orchestration + Logging

---

## Tech Stack

- Open-Meteo APIs (Geocoding + Historical Weather)  
- DuckDB (analytical warehouse)  
- dbt + dbt-duckdb (ELT transformations)  
- uv (Python environment + fast package runner)  
- Prefect 3 (workflow orchestration)  
- scikit-learn (baseline ML model)  
- Streamlit + Plotly (interactive analytics)  
- pytest (testing)  
- YAML configuration system  
- Custom CLI entrypoints  

---

## Data Overview

### Cities

| city_id | city_name     | country_code |
|--------:|---------------|--------------|
| 1       | Los Angeles   | US |
| 2       | San Francisco | US |
| 3       | Lisbon        | PT |
| 4       | Porto         | PT |

Metadata is sourced via the Open-Meteo Geocoding API and stored in:

- data/raw/geocoding/  
- dbt/seeds/dim_city.csv

### Daily Variables

- temperature_2m_max  
- temperature_2m_min  
- temperature_2m_mean  
- dew_point_2m_mean  
- precipitation_sum  
- wind_speed_10m_max  
- shortwave_radiation_sum  

**Time window:**  
    1980-01-01 → 2024-12-31

All raw data is preserved unchanged.

---

## Warehouse Models (dbt)

### Bronze – `main.bronze_daily_weather`

One row per city and date.

Flattened from JSON using:
- read_json_auto  
- array unnesting  
- join to dim_city  

---

### Silver Layer

**main.silver_daily_weather_features**

Adds:
- heat day flags  
- tropical night flags  
- heavy precipitation flags  
- seasonal indicators  

**main.silver_monthly_climate**

Monthly aggregation:
- avg temperature  
- precipitation  
- heat/tropical night counts  
- wind/radiation summaries  

---

## Gold Layer

**main.climatology_city_month** – baseline climatology.  

**main.gold_city_month_anomalies** – anomaly scores & z-scores.  

**main.gold_city_anomaly_lags** – lagged deltas (1–3 months).  

**main.gold_city_anomaly_correlations** – cross-city correlations.  

**main.gold_city_anomaly_events** – binary climate events.  

**main.gold_ml_features** – final ML feature store.  

---

## Machine Learning Layer

Baseline:

- RandomForestClassifier  
- Target: is_positive_temp_anomaly  
- Time-based split  
- Imbalanced but realistic labels  

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

Features:
- city selector  
- metric selector  
- monthly trend plots  
- anomaly visualization  
- cross-city comparison  

---

## Orchestration

Run everything:

    uv run climate-run-all

With tests:

    uv run climate-run-all --with-tests

Steps:
1. DuckDB lock check  
2. dbt build (bronze → silver → gold)  
3. ML training  
4. pytest (optional)  

---

## Testing

    uv run pytest

Validates:
- Gold table schemas  
- Feature completeness  
- No NaN leakage  
- Integrity constraints  

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

## Planned Enhancements

- Transformer/LSTM temporal models  
- Extreme event classifiers  
- Correlation heatmaps  
- Sea surface temperature integration  
- Docker + cloud deployment  
- Public dashboard hosting  