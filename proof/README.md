# Evidence Manifest

This directory stores validation metadata for the latest saved artifact set. The
directory name remains `proof/` for compatibility with the existing scripts.

## Files
- `evidence_contract.schema.json`
- `evidence_manifest.latest.json`
- `proof_points.latest.md`
- `test_summary.latest.json`
- `generate_canonical_manifest.py`
- `validate_evidence_manifest.py`

## Validation Run

```bash
uv sync --dev
uv run climate-init-observability
uv run climate-dbt-build
uv run climate-train-baseline
uv run climate-predict-baseline
cd dbt
uv run dbt build --project-dir . --profiles-dir . --select observability
cd ..
uv run python -m pytest -m "not integration"
uv run python -m pytest -m integration
python proof/generate_canonical_manifest.py
python proof/validate_evidence_manifest.py
```

## Validate The Saved Manifest

```bash
python proof/validate_evidence_manifest.py
```
