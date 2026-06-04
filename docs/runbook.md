# Runbook

This runbook covers local setup, pipeline execution, dashboard startup, artifact
inspection, validation, and cleanup.

For config, data, warehouse, model, and artifact contracts, see
[Workflow Interface](interface.md).

## Setup

```bash
uv sync --dev
```

## Build Warehouse

```bash
uv run climate-dbt-build
```

Equivalent explicit command:

```bash
cd dbt
uv run dbt build --project-dir . --profiles-dir .
```

If observability base tables are missing, initialize them explicitly:

```bash
uv run climate-init-observability
```

## Run Ingestion

Recent mode:

```bash
uv run fetch-daily-weather --mode recent
```

Backfill mode:

```bash
uv run fetch-daily-weather --mode backfill
```

## Train And Predict

```bash
uv run climate-train-baseline
uv run climate-predict-baseline
```

Inspect:

- `models/baseline_rf_metrics.json`
- `data/mart/predictions/baseline_rf_predictions.csv`

## Run Orchestration

Daily flow:

```bash
uv run python -m climate_pipeline.orchestration.prefect_flow daily
```

Backfill flow:

```bash
uv run python -m climate_pipeline.orchestration.prefect_flow backfill \
  --start-date 1980-01-01 \
  --end-date 2024-12-31
```

Optional checks:

```bash
uv run python -m climate_pipeline.orchestration.prefect_flow daily \
  --with-dbt-tests \
  --with-tests
```

## Run Tests

Fast tests:

```bash
uv run python -m pytest -m "not integration"
```

Integration tests:

```bash
uv run python -m pytest -m integration
```

Integration tests require a built DuckDB database. If observability views are
missing, rebuild the observability dbt models before running the integration
suite.

## Run Dashboard

```bash
uv run streamlit run dashboards/streamlit/app.py
```

## Validate Saved Artifact References

```bash
python proof/validate_evidence_manifest.py
```

Refresh manifest timestamps and hashes after intentionally updating curated
artifacts:

```bash
python proof/generate_canonical_manifest.py
```

## Cleanup

There is no long-running service to shut down unless Streamlit is running. Stop
Streamlit with `Ctrl-C` in the terminal where it is running.

Generated runtime state can be rebuilt from pipeline commands. Before deleting
anything, check whether the file is intentionally tracked as a saved artifact.
