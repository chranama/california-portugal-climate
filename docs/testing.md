# Testing

The test suite checks warehouse models, ML feature structure, model training,
observability models, and path resolution.

## Setup

```bash
uv sync --dev
```

## Fast Tests

Fast tests do not require a built DuckDB warehouse:

```bash
uv run python -m pytest -m "not integration"
```

Use Python module invocation rather than a local `pytest` console script. This
avoids relying on a stale script if a virtual environment was moved between
directories.

## Integration Tests

Integration tests require a built warehouse and are marked with `integration`:

```bash
uv run python -m pytest -m integration
```

The CI integration job builds synthetic weather fixtures, initializes
observability tables, runs dbt, trains the baseline model, writes predictions,
rebuilds observability models, and then runs this test subset.

## Test Categories

Current tests cover:

- landing table existence and schema
- clean daily and monthly table schema
- anomaly table existence, schema, and non-empty outputs
- ML feature table schema, target labels, and feature null checks
- baseline model training smoke behavior
- observability view existence and summary columns
- environment and path resolution

## Warehouse Requirements

Tests that query DuckDB require `data/warehouse/climate.duckdb`.

Build the warehouse:

```bash
uv run climate-dbt-build
```

The observability tests require base observability tables and dbt observability
models:

- `pipeline_run_log`
- `pipeline_ml_metrics`
- `pipeline_runs`
- `pipeline_run_daily_summary`
- `pipeline_ml_daily_summary`

Initialize base tables when needed:

```bash
uv run climate-init-observability
```

If observability views are missing from an existing local warehouse, rebuild the
observability layer:

```bash
cd dbt
uv run dbt build --project-dir . --profiles-dir . --select observability
```

## Test Isolation

The ML training smoke test writes model and metric outputs to pytest temporary
paths. It should not update the curated artifacts under `models/`.

## Evidence Manifest

Validate saved artifact references and SHA-256 hashes:

```bash
python proof/validate_evidence_manifest.py
```
