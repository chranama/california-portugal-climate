# Evidence Summary (Latest)

## Evidence 1: Baseline Model + Metrics
- Claim: baseline anomaly model outputs are persisted and inspectable.
- Command: `python proof/generate_canonical_manifest.py`
- Artifacts:
  - `models/baseline_rf.pkl`
  - `models/baseline_rf_metrics.json`
- Validation signal: both files exist.

## Evidence 2: Prediction Sample
- Claim: prediction outputs are available for concrete inspection.
- Command: `python proof/generate_canonical_manifest.py`
- Artifacts:
  - `data/mart/predictions/baseline_rf_predictions.csv`
- Validation signal: CSV exists and is non-empty.

## Evidence 3: Reliability Test Evidence
- Claim: key anomaly and feature integrity test evidence is represented in the saved artifact set.
- Command: `python proof/validate_evidence_manifest.py`
- Artifacts:
  - `tests/test_anomaly_layer.py`
  - `tests/test_ml_features.py`
  - `proof/test_summary.latest.json`
- Validation signal: required test files and test summary artifact exist.

## Evidence 4: Local Verification State
- Claim: latest local verification state is captured for inspection.
- Command: `uv run climate-verify-local`
- Artifacts:
  - `proof/workflow_state.latest.json`
- Validation signal: workflow-state artifact exists and records command outcomes, warehouse row counts, model metrics, prediction count, and observability summaries.
