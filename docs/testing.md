# Testing

The test suite checks warehouse models, ML feature structure, model training,
observability models, and path resolution.

## Setup

Use the runbook for local environment setup.

## Fast Tests

Fast tests do not require a built DuckDB warehouse. Use the runbook for the
local test command.

## Integration Tests

Integration tests require a built warehouse and are marked with `integration`:

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

The observability tests require base observability tables and dbt observability
models:

- `pipeline_run_log`
- `pipeline_ml_metrics`
- `pipeline_runs`
- `pipeline_run_daily_summary`
- `pipeline_ml_daily_summary`

Use the runbook for warehouse build, observability initialization, and
observability-layer rebuild commands.

## Test Isolation

The ML training smoke test writes model and metric outputs to pytest temporary
paths. It should not update the curated artifacts under `models/`.

## Evidence Manifest

Evidence manifest validation checks saved artifact references and SHA-256
hashes. Use the runbook for the local validation command.

`uv run climate-verify-local` runs the fast and integration pytest subsets, writes
`proof/test_summary.latest.json`, writes `proof/workflow_state.latest.json`, and
refreshes the evidence manifest hashes.
