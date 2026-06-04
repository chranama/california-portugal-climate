# Workflow Interface

California-Portugal Climate Pipeline is a local file, warehouse, and dashboard
workflow rather than a web API. The public interface is the city configuration,
runtime settings, Open-Meteo inputs, dbt model layers, ML feature table, and
saved artifacts.

The runbook contains runnable command sequences. This document describes what
those commands consume and what they produce.

## City Configuration

The configured comparison set lives in `src/config/cities.yaml`.

Current cities:

| city_id | name | country_code |
|---:|---|---|
| 1 | Los Angeles | US |
| 2 | San Francisco | US |
| 3 | Lisbon | PT |
| 4 | Porto | PT |

The city ids are used across raw files, dbt seed data, warehouse tables, model
features, predictions, and dashboard views.

## Runtime Settings

Project settings live in `src/config/settings.yaml`. Paths are resolved from
environment variables so the same code can run locally or in a container.

Important settings:

| Setting | Purpose |
|---|---|
| `CLIMATE_DATA_ROOT` | Root for raw data, warehouse files, and prediction outputs. |
| `CLIMATE_LOG_ROOT` | Root for ingestion and pipeline logs. |
| `OPEN_METEO_GEOCODING_BASE_URL` | Base URL for city geocoding. |
| `OPEN_METEO_HISTORICAL_BASE_URL` | Base URL for historical daily weather. |
| `data.raw_weather_dir` | Raw Open-Meteo daily response directory. |
| `data.warehouse_path` | DuckDB warehouse path. |
| `time_window.start_date` and `time_window.end_date` | Backfill window for historical ingestion. |

Open-Meteo daily variables requested by the pipeline:

- `temperature_2m_max`
- `temperature_2m_min`
- `temperature_2m_mean`
- `dew_point_2m_mean`
- `precipitation_sum`
- `wind_speed_10m_max`
- `shortwave_radiation_sum`

## Data Flow Contract

The workflow moves data through these boundaries:

| Stage | Input | Output |
|---|---|---|
| Ingestion | city config, Open-Meteo API, time window | raw daily weather files under `data/raw/` |
| Landing dbt models | raw weather files | normalized DuckDB landing tables |
| Clean dbt models | landing tables | daily and monthly climate tables |
| Anomaly dbt models | clean monthly climate tables | climatology, anomaly, event, lag, and correlation tables |
| ML dbt model | anomaly tables | `ml_features` table |
| Training | `ml_features` table | model pickle and metrics JSON |
| Prediction | saved model and `ml_features` table | prediction CSV sample |
| Dashboard | warehouse tables and saved artifacts | local Streamlit views |
| Evidence validation | saved artifacts and manifest | validation result for current artifact references |

## Warehouse Tables

The dbt project is under `dbt/`.

Important model layers:

| Layer | Purpose |
|---|---|
| `landing` | Normalize raw Open-Meteo daily weather records. |
| `clean` | Build validated daily features and monthly climate summaries. |
| `anomaly` | Build climatology, anomaly scores, event labels, lags, and cross-city correlations. |
| `ml` | Build the supervised feature table used by the baseline model. |
| `observability` | Summarize pipeline runs and ML metric logging. |

The main ML table is `ml_features`, defined in `dbt/models/ml/ml_features.sql`.

Feature columns used by the saved baseline model:

- `anomaly_tmean_c`
- `roll_mean_3`
- `roll_mean_6`
- `roll_std_3`
- `roll_std_6`
- `delta_1m`
- `delta_3m`
- `max_lagged_corr`
- `lead_lag_months`
- `sin_month`
- `cos_month`

Target column:

- `is_event_next_month`

## Model Metrics Output

The saved baseline metrics live at `models/baseline_rf_metrics.json`.

Important fields:

| Field | Meaning |
|---|---|
| `accuracy` | Overall test accuracy for the saved train/test split. |
| `roc_auc` | ROC AUC for the event classifier. |
| `classification_report` | Per-class precision, recall, F1, and support. |
| `n_train` and `n_test` | Train and test row counts. |
| `feature_columns` | Feature set used by the model. |
| `target_column` | Supervised target label. |
| `class_distribution_test` | Test-set class counts. |

Concrete saved metric values include:

```json
{
  "accuracy": 0.9508196721311475,
  "roc_auc": 0.6153682418050234,
  "n_train": 1647,
  "n_test": 549,
  "target_column": "is_event_next_month"
}
```

The high accuracy should be read together with the class distribution and
minority-class recall. It is a baseline workflow artifact, not a climate science
performance claim.

## Prediction Output

The saved prediction sample lives at:

- `data/mart/predictions/baseline_rf_predictions.csv`

Columns:

| Column | Meaning |
|---|---|
| `city_id` | City identifier. |
| `city_name` | City name. |
| `year` | Prediction year. |
| `month` | Prediction month. |
| `anomaly_tmean_c` | Monthly mean temperature anomaly in Celsius. |
| `pred_event_next_month` | Predicted next-month strong anomaly event label. |
| `prob_event_next_month` | Predicted event probability. |

Example row shape:

```csv
city_id,city_name,year,month,anomaly_tmean_c,pred_event_next_month,prob_event_next_month
4,Porto,1980,4,0.5657777777777824,0,0.3262117269585982
```

## Evidence Outputs

The current saved artifact set is tracked through:

- `proof/evidence_manifest.latest.json`
- `proof/evidence_contract.schema.json`
- `proof/proof_points.latest.md`
- `proof/test_summary.latest.json`

The runbook describes how to validate these references locally.

## Interface Limits

The workflow interface is local and file-based. It does not expose a network
API, production scheduler, managed warehouse, hosted dashboard, or live
forecasting system. The saved artifacts show a bounded, inspectable data and ML
pipeline state.
