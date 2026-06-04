# Artifacts

The repository includes saved artifacts so the current local pipeline state can
be inspected without rerunning every step.

## What To Inspect First

- `models/baseline_rf_metrics.json`: saved baseline model metrics.
- `data/mart/predictions/baseline_rf_predictions.csv`: saved prediction sample.
- `proof/evidence_manifest.latest.json`: machine-readable saved artifact inventory.
- `proof/test_summary.latest.json`: latest test summary metadata.
- `dbt/models/ml/ml_features.sql`: feature and target definition for the model.
- `dbt/models/anomaly/`: climatology, anomaly, lag, event, and correlation models.
- `dashboards/streamlit/app.py`: local dashboard entry point and view logic.

## Model Artifacts

- `models/baseline_rf.pkl`: saved RandomForest baseline model
- `models/baseline_rf_metrics.json`: model evaluation metrics

These files show the current saved baseline state. They can change when the
training command is rerun against a different warehouse state. The integration
test suite writes smoke-test model outputs to temporary paths so normal test runs
do not update these curated files.

## Prediction Artifact

- `data/mart/predictions/baseline_rf_predictions.csv`

This CSV is a saved prediction sample from the baseline model.

## Evidence Manifest

The `proof/` directory contains validation metadata for the latest saved
artifact set.

Primary files:

- `proof/evidence_contract.schema.json`
- `proof/evidence_manifest.latest.json`
- `proof/proof_points.latest.md`
- `proof/test_summary.latest.json`
- `proof/generate_canonical_manifest.py`
- `proof/validate_evidence_manifest.py`

The validator checks artifact presence, non-empty files, required diagnostic
metadata, and SHA-256 hashes recorded in `evidence_manifest.latest.json`.

The runbook contains the local validation command.

## Generated Files

The normal pipeline also produces local data, logs, dbt targets, and DuckDB
state. Those are runtime outputs, not source files.

Examples:

- `data/raw/`
- `data/warehouse/climate.duckdb`
- `dbt/target/`
- `dbt/logs/`
- `logs/`
- uncurated files under `models/`
- uncurated files under `data/mart/`

## Artifact Limits

Saved artifacts reflect a bounded local run. They do not establish production
reliability, climate forecasting validity, or model performance across a broad
range of locations and time windows.
