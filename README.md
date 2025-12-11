# California–Portugal Climate Pipeline  
### A Full-Stack Climate Data Engineering, Analytics & Machine Learning System

This project implements an end-to-end climate data platform comparing four coastal Mediterranean-like cities:

- Los Angeles (US)  
- San Francisco (US)  
- Lisbon (PT)  
- Porto (PT)

Using daily historical weather data from 1980–present, the system ingests, transforms, models, and analyzes long-term climate patterns and anomaly events. It produces:

- A production-ready DuckDB warehouse  
- A multi-layer dbt pipeline  
- An ML-ready feature store  
- A baseline RandomForest anomaly model  
- A Streamlit analytics dashboard  
- Prefect orchestration (daily + backfill workflows)  
- Automated data quality tests  
- Full observability logging (pipeline + ML metrics)

---

## Architecture Overview (Mermaid Diagram)

flowchart TD
    A[Open-Meteo API] --> B[Landing Layer<br>raw JSON]
    B --> C[Clean Layer<br>validated daily tables]
    C --> D[Anomaly Layer<br>climatology + anomaly scoring]
    D --> E[ML Feature Layer<br>lagged monthly features]
    E --> F[ML Models<br>Random Forest baseline]
    E --> G[Streamlit Dashboard]

    subgraph Orchestration
        H[Prefect Flows<br>daily + backfill]
    end

    H --> B
    H --> C
    H --> D
    H --> E
    H --> F

---

## Pipeline Layers (New Naming Framework)

### 1. Landing Layer (formerly Bronze)
Structured Open-Meteo responses stored as raw-in/normalized-out weather tables.

Contents:
- daily weather fields  
- geocoding metadata  
- ingestion metadata  

---

### 2. Clean Layer (formerly Silver)
Validated, enriched daily + monthly weather summaries. Adds:

- extreme temperature flags  
- tropical night detection  
- heavy precipitation flags  
- seasonal encodings  
- data quality normalization  

---

### 3. Anomaly Layer (formerly Gold)
High-level domain climate analytics:

- climatology baselines  
- standardized anomaly scoring (z-scores)  
- anomaly event detection  
- temporal lags  
- cross-city anomaly correlations  

---

### 4. ML Feature Store
Final engineered feature matrix for supervised anomaly prediction:

Includes:
- rolling-window statistics  
- multiple lag deltas  
- teleconnection-style lag summary  
- seasonal encodings  
- target label: is_event_next_month  

---

## Machine Learning Layer

Model:
- RandomForestClassifier  
- Predicts next-month strong anomaly events

Artifacts:
- models/baseline_rf.pkl  
- models/baseline_rf_metrics.json  

Run training:
uv run climate-train-baseline

---

## Streamlit Dashboard

Location:
dashboards/streamlit/app.py

Run:
uv run streamlit run dashboards/streamlit/app.py

Features:
- Climate trend exploration  
- Anomaly visualization  
- ML performance  
- Pipeline health metrics  
- Freshness indicators  

---

## Orchestration (Prefect)

Daily pipeline:
uv run python -m climate_pipeline.orchestration.prefect_flow daily

Backfill:
uv run python -m climate_pipeline.orchestration.prefect_flow backfill --start-date YYYY-MM-DD --end-date YYYY-MM-DD

Pipeline steps:
1. Fetch daily weather  
2. dbt build (all layers)  
3. Optional dbt tests  
4. Train ML model  
5. Optional pytest  
6. Observability logging  

---

## Testing

uv run pytest

Includes:
- schema validation  
- anomaly layer logic tests  
- ML feature integrity tests  
- observability table tests  
- utilities + path resolution tests  

---

## Project Structure (Simplified)

california-portugal-climate/  
    README.md  
    pyproject.toml  
    data/  
        raw/  
        warehouse/climate.duckdb  
    dbt/  
    src/climate_pipeline/  
    dashboards/streamlit/  
    tests/  
    logs/  

---

## Quick Start

uv sync  
uv run climate-train-baseline  
uv run streamlit run dashboards/streamlit/app.py  

---

## Planned Enhancements

- Teleconnection indices (ENSO, NAO, PDO, AMO)  
- Sea-surface-temperature integration  
- Reanalysis integration (ERA5, NOAA)  
- Short-term forecasting (3-day max/min)  
- Transformer/LSTM anomaly models  
- Expanded observability dashboards  
- Docker + cloud deployment  