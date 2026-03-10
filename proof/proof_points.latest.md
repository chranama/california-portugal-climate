# Canonical Proof Points (Latest)

## Proof 1: Baseline Model + Metrics
- Claim: baseline anomaly model outputs are persisted and reviewable.
- Command: `python proof/generate_canonical_manifest.py`
- Artifacts:
  - `models/baseline_rf.pkl`
  - `models/baseline_rf_metrics.json`
- Validation signal: both files exist.

## Proof 2: Prediction Sample
- Claim: prediction outputs are available for concrete inspection.
- Command: `python proof/generate_canonical_manifest.py`
- Artifacts:
  - `data/mart/predictions/baseline_rf_predictions.csv`
- Validation signal: CSV exists and is non-empty.

## Proof 3: Reliability Test Evidence
- Claim: key anomaly and feature integrity test evidence is represented in canonical bundle.
- Command: `python proof/validate_evidence_manifest.py`
- Artifacts:
  - `tests/test_anomaly_layer.py`
  - `tests/test_ml_features.py`
  - `proof/test_summary.latest.json`
- Validation signal: required test files and test summary artifact exist.
