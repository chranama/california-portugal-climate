# Pipeline

The pipeline combines data ingestion, dbt transformations, baseline model
training, prediction, observability logging, and dashboard inspection.

Use the runbook for exact local command sequences. Use
[Workflow Interface](interface.md) for config, input, output, and artifact
contracts.

## Cities

The configured comparison set is:

- Los Angeles, United States
- San Francisco, United States
- Lisbon, Portugal
- Porto, Portugal

City metadata is stored in `src/config/cities.yaml` and dbt seed data.

## Concrete Flow Example

For a configured city such as Porto, the ingestion step fetches Open-Meteo daily
weather records and writes raw files under the local data root. dbt landing
models normalize those records into DuckDB. Clean models create daily and
monthly climate rows. Anomaly models compare monthly temperatures against local
climatology and create lag features. The ML model uses those rows to predict
whether the next month is a strong anomaly event. The saved prediction sample and
dashboard views then expose the result for inspection.

## Ingestion

Ingestion reads city configuration and fetches daily weather data from
Open-Meteo.

The ingestion script validates daily response structure, checks required fields,
and retries requests before writing raw weather files.

## dbt Layers

The dbt project contains:

- `landing`: normalized raw weather tables
- `clean`: validated daily and monthly summaries
- `anomaly`: climatology, anomaly scores, event labels, lags, and correlations
- `ml`: final ML feature table
- `observability`: run and ML metric summary views

## Machine Learning

The baseline model is a RandomForest classifier trained against the `ml_features`
table.

Training writes:

- `models/baseline_rf.pkl`
- `models/baseline_rf_metrics.json`

Prediction writes a sample output under
`data/mart/predictions/baseline_rf_predictions.csv`.

## Orchestration

The daily and backfill flows run ingestion, dbt build, model training, and
best-effort run logging.
Optional flags can run dbt tests and pytest after the main workflow steps.

## Dashboard

The dashboard reads the local warehouse and saved artifacts to show climate
trends, anomaly behavior, model metrics, and pipeline health.
