# Pipeline

The pipeline combines data ingestion, dbt transformations, baseline model
training, prediction, observability logging, and dashboard inspection.

## Cities

The configured comparison set is:

- Los Angeles, United States
- San Francisco, United States
- Lisbon, Portugal
- Porto, Portugal

City metadata is stored in `src/config/cities.yaml` and dbt seed data.

## Ingestion

Ingestion reads city configuration and fetches daily weather data from
Open-Meteo.

Recent mode:

```bash
uv run fetch-daily-weather --mode recent
```

Backfill mode:

```bash
uv run fetch-daily-weather --mode backfill
```

The ingestion script validates daily response structure, checks required fields,
and retries requests before writing raw weather files.

## dbt Layers

Build the dbt project from the repository root:

```bash
uv run climate-dbt-build
```

The dbt project contains:

- `landing`: normalized raw weather tables
- `clean`: validated daily and monthly summaries
- `anomaly`: climatology, anomaly scores, event labels, lags, and correlations
- `ml`: final ML feature table
- `observability`: run and ML metric summary views

## Machine Learning

The baseline model is a RandomForest classifier trained against the `ml_features`
table.

```bash
uv run climate-train-baseline
```

Training writes:

- `models/baseline_rf.pkl`
- `models/baseline_rf_metrics.json`

Prediction writes a sample output:

```bash
uv run climate-predict-baseline
```

## Orchestration

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

Both flows run ingestion, dbt build, model training, and best-effort run logging.
Optional flags can run dbt tests and pytest after the main workflow steps.

## Dashboard

Run the Streamlit dashboard locally:

```bash
uv run streamlit run dashboards/streamlit/app.py
```

The dashboard reads the local warehouse and saved artifacts to show climate
trends, anomaly behavior, model metrics, and pipeline health.
