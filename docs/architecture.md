# Architecture

The project is a local data and ML workflow built around DuckDB and dbt. Python
entry points handle ingestion, orchestration, model training, prediction,
observability logging, and dashboard startup.

## Component Flow

```text
Open-Meteo API
  -> climate_pipeline.ingestion
  -> data/raw/
  -> dbt landing models
  -> dbt clean models
  -> dbt anomaly models
  -> dbt ml model
  -> climate_pipeline.ml
  -> models/ and data/mart/predictions/
  -> Streamlit dashboard and observability views
```

## Main Boundaries

- `src/climate_pipeline/ingestion`: Open-Meteo request handling, city
  configuration, validation, retry behavior, and raw file writing.
- `dbt/models/landing`: raw weather normalization into warehouse tables.
- `dbt/models/clean`: daily and monthly clean climate summaries.
- `dbt/models/anomaly`: climatology, anomaly scores, event labels, lags, and
  cross-city correlations.
- `dbt/models/ml`: final supervised feature table for anomaly prediction.
- `dbt/models/observability`: pipeline and ML run summary views.
- `src/climate_pipeline/ml`: baseline model training and prediction.
- `src/climate_pipeline/observability`: run and metric logging into DuckDB.
- `src/climate_pipeline/orchestration`: local and Prefect orchestration flows.
- `dashboards/streamlit`: local dashboard for analytics and pipeline health.

## Runtime Boundaries

The workflow uses files and DuckDB as its primary runtime boundary:

- raw API responses are written under `data/raw/`
- dbt builds warehouse models into `data/warehouse/climate.duckdb`
- training reads `ml_features` from DuckDB
- model and metric artifacts are written under `models/`
- predictions are written under `data/mart/predictions/`
- observability records are written back into DuckDB

This keeps the project inspectable locally, but it also means the repository is
not a multi-user data platform or hosted service.

## Configuration

Project-level configuration lives in:

- `src/config/cities.yaml`
- `src/config/settings.yaml`
- `dbt/profiles.yml`
- environment variables such as `CLIMATE_DATA_ROOT`, `CLIMATE_LOG_ROOT`,
  `DUCKDB_PATH`, and `DBT_PROFILES_DIR`

The utility layer resolves these paths so command-line and orchestration entry
points use the same data, log, and warehouse locations.

## Design Tradeoffs

The repository favors explicit local tools over a cloud platform: DuckDB instead
of a managed warehouse, dbt instead of ad hoc SQL scripts, and Prefect flows
instead of a deployed scheduler. That makes the workflow easier to inspect and
run locally, while leaving production deployment, high availability, and remote
data governance out of scope.
