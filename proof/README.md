# Proof System

Latest-only canonical evidence bundle for reviewer verification.

## Files
- `evidence_contract.schema.json`
- `evidence_manifest.latest.json`
- `proof_points.latest.md`
- `test_summary.latest.json`
- `generate_canonical_manifest.py`
- `validate_evidence_manifest.py`

## Canonical Proof Run

```bash
uv sync
uv run climate-train-baseline
uv run pytest tests/test_anomaly_layer.py tests/test_ml_features.py
python proof/generate_canonical_manifest.py
```

## Validate

```bash
python proof/validate_evidence_manifest.py
```
