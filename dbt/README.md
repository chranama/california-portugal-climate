# dbt Layer (Climate Pipeline)

This directory contains the warehouse transformation layer for the California-Portugal climate project.

## Models

- `landing/`: raw Open-Meteo normalization
- `clean/`: validated and enriched daily/monthly climate tables
- `anomaly/`: climatology baselines, anomaly scores, lag features, event labels
- `ml/`: ML feature table (`ml_features`)
- `observability/`: pipeline and ML run summary views

## Run from repo root

```bash
uv sync
uv run climate-dbt-build
```

Equivalent explicit dbt commands:

```bash
cd dbt
uv run dbt run
uv run dbt test
```

## Dependency Notes

- `climate-train-baseline` expects `ml_features` to exist.
- `ml_features` is produced by this dbt layer.
- If dbt models are not built first, baseline training will fail on clean setups.
