# Scope

California-Portugal Climate Pipeline is a local data engineering and ML workflow
for comparing weather and anomaly patterns across four configured cities.

## In Scope

- Open-Meteo weather ingestion
- Local DuckDB warehouse
- dbt landing, clean, anomaly, ML, and observability models
- Baseline RandomForest anomaly classification
- Saved metrics and prediction sample
- Local Prefect flows for daily and backfill workflows
- Streamlit dashboard for local inspection
- Tests for table shape, features, training smoke behavior, observability, and
  path resolution
- Evidence manifest validation for saved artifacts

## Out Of Scope

- Production deployment
- High-availability scheduling
- Multi-user data platform operation
- Climate forecasting service
- Full climate science benchmark
- Model serving API
- Security hardening
- Cloud data warehouse governance
- Long-term monitoring or alerting

## Current Limits

- The configured location set is four cities.
- The baseline model is a simple RandomForest classifier.
- Current baseline metrics should be read as workflow evidence, not as evidence
  of strong positive-event detection.
- Saved artifacts represent a local run state and may change when commands are
  rerun.
- Warehouse-dependent tests require the DuckDB file and expected dbt models.
- Observability tests require observability views to exist in the local
  warehouse.
- Runtime outputs and source files need a clear artifact policy if this repo is
  used as a long-term public evidence surface.
