# Evidence Manifest

This directory stores validation metadata for the latest saved artifact set. The
directory name remains `proof/` for compatibility with the existing scripts.

## Files
- `evidence_contract.schema.json`
- `evidence_manifest.latest.json`
- `proof_points.latest.md`
- `workflow_state.latest.json`
- `test_summary.latest.json`
- `generate_canonical_manifest.py`
- `validate_evidence_manifest.py`

## Validation Run

```bash
uv sync --dev
uv run climate-verify-local
```

## Validate The Saved Manifest

```bash
python proof/validate_evidence_manifest.py
```
