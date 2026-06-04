# California-Portugal Climate Pipeline

California-Portugal Climate Pipeline is a local data and machine learning
workflow for comparing historical weather patterns across Los Angeles, San
Francisco, Lisbon, and Porto. It ingests Open-Meteo weather data, transforms it
through dbt models in DuckDB, builds anomaly features, trains a baseline model,
and exposes results through saved artifacts and a Streamlit dashboard.

Open-Meteo provides historical weather measurements for the configured cities.
DuckDB and dbt turn those raw API responses into queryable climate tables. The
machine learning step trains a local baseline anomaly classifier so the workflow
can be inspected end to end; it is not a climate forecast.

The repository is intended for local data engineering and ML workflow review. It
is not a production deployment, a climate forecasting service, or a complete
climate science benchmark.

## Workflow

```text
Open-Meteo API
  -> raw weather files
  -> DuckDB landing models
  -> clean daily and monthly climate tables
  -> anomaly and lag feature models
  -> ML feature table
  -> RandomForest anomaly baseline
  -> saved metrics, predictions, observability tables, and dashboard views
```

## Responsibilities

- Fetch historical and recent daily weather data for the configured cities
- Normalize raw API responses into DuckDB through dbt
- Build clean, anomaly, ML feature, and observability warehouse layers
- Train and evaluate a baseline RandomForest anomaly model
- Persist model metrics, prediction samples, and run metadata
- Provide Prefect flows for daily and backfill workflows
- Expose a Streamlit dashboard for trends, anomalies, model metrics, and
  pipeline health
- Validate key tables, features, paths, and saved artifacts through tests

## Repository Layout

```text
src/climate_pipeline/   Python package for ingestion, ML, orchestration, health checks, and utilities
dbt/                    DuckDB/dbt transformation project
dashboards/streamlit/   Streamlit dashboard
tests/                  Warehouse, ML, observability, and utility tests
data/                   Local data roots and saved prediction sample
models/                 Saved baseline model and metrics artifacts
proof/                  Evidence manifest validation scripts and metadata
docs/                   System documentation
```

## Run Locally

The local runbook provides the step-by-step guide for setup, ingestion, dbt
builds, model training, prediction, dashboard startup, tests, artifact
validation, and cleanup:

- [Runbook](docs/runbook.md)

## Current Outputs

Saved outputs that are useful for inspection:

- `models/baseline_rf_metrics.json`
- `models/baseline_rf.pkl`
- `data/mart/predictions/baseline_rf_predictions.csv`
- `proof/evidence_manifest.latest.json`
- `proof/test_summary.latest.json`

These artifacts show a bounded local pipeline state: model metrics, prediction
samples, evidence metadata, and test summary metadata. They are not a substitute
for rerunning the pipeline against fresh data.

## Documentation

- [Architecture](docs/architecture.md)
- [Workflow Interface](docs/interface.md)
- [Pipeline](docs/pipeline.md)
- [Testing](docs/testing.md)
- [Artifacts](docs/artifacts.md)
- [Runbook](docs/runbook.md)
- [Scope](docs/scope.md)

## License

Released under the MIT License.
