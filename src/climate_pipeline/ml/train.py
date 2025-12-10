"""
Baseline ML training script for climate anomaly events.

Usage (from project root):

  uv run python -m climate_pipeline.ml.train

or

  uv run python src/climate_pipeline/ml/train.py

You can override defaults, e.g.:

  uv run python -m climate_pipeline.ml.train \
    --db-path data/warehouse/climate.duckdb \
    --table-name gold_ml_features \
    --model-path models/baseline_rf.pkl \
    --metrics-path models/baseline_rf_metrics.json \
    --train-fraction 0.75
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    roc_auc_score,
)

from climate_pipeline.observability.run_logger import (
    MLMetricRecord,
    log_ml_metrics,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TARGET_COL = "is_event_next_month"

FEATURE_COLS: List[str] = [
    "anomaly_tmean_c",
    "roll_mean_3",
    "roll_mean_6",
    "roll_std_3",
    "roll_std_6",
    "delta_1m",
    "delta_3m",
    "max_lagged_corr",
    "lead_lag_months",
    "sin_month",
    "cos_month",
]

# ML observability defaults (can be overridden via env if you want)
DEFAULT_FLOW_NAME = os.getenv("PIPELINE_FLOW_NAME", "daily-climate-pipeline")
DEFAULT_RUN_MODE = os.getenv("PIPELINE_RUN_MODE", "daily")
DEFAULT_MODEL_NAME = os.getenv("MODEL_NAME", "baseline_random_forest")
DEFAULT_MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")


# ---------------------------------------------------------------------------
# Data loading & preparation
# ---------------------------------------------------------------------------

def load_ml_table(db_path: Path, table_name: str) -> pd.DataFrame:
    """Load the ML features table from DuckDB."""
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(f'SELECT * FROM "{table_name}"').df()
    finally:
        con.close()

    if df.empty:
        raise ValueError(f"Table {table_name} is empty.")

    return df


def prepare_data(
    df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str,
    train_fraction: float = 0.75,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, pd.DataFrame]:
    """
    Clean and split the data for training.

    Steps:
      - drop rows with NaNs in any feature column
      - sort by (city_id, year, month)
      - split into train/test along the time axis
    """
    missing_cols = [c for c in feature_cols + [target_col] if c not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing expected columns in dataframe: {missing_cols}")

    # Drop rows with NaNs in any feature
    df_clean = df.dropna(subset=feature_cols).copy()

    # Sort to preserve time order
    df_clean = df_clean.sort_values(["city_id", "year", "month"]).reset_index(drop=True)

    X = df_clean[feature_cols].to_numpy()
    y = df_clean[target_col].to_numpy()

    if not (0.0 < train_fraction < 1.0):
        raise ValueError(f"train_fraction must be between 0 and 1, got {train_fraction}")

    cutoff = int(len(df_clean) * train_fraction)
    if cutoff == 0 or cutoff == len(df_clean):
        raise ValueError("Train/test split produced an empty split; adjust train_fraction.")

    X_train, X_test = X[:cutoff], X[cutoff:]
    y_train, y_test = y[:cutoff], y[cutoff:]

    return X_train, X_test, y_train, y_test, df_clean


# ---------------------------------------------------------------------------
# Model training & evaluation
# ---------------------------------------------------------------------------

def train_random_forest(
    X_train: np.ndarray,
    y_train: np.ndarray,
    n_estimators: int = 300,
    max_depth: int | None = 8,
    random_state: int = 42,
) -> RandomForestClassifier:
    """Train a random forest with class_weight='balanced' as the baseline model."""
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        class_weight="balanced",
        random_state=random_state,
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(
    model: RandomForestClassifier,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> dict:
    """
    Compute classification metrics for the baseline model.

    Returns a dict with:
      - accuracy
      - roc_auc (if available)
      - per-class precision/recall/f1 (for labels '0' and '1' if present)
      - n_test and class_distribution_test
    """
    y_pred = model.predict(X_test)

    # Try to get probabilities for ROC-AUC
    y_prob = None
    try:
        y_prob = model.predict_proba(X_test)[:, 1]
    except Exception:
        # Some classifiers may not support predict_proba
        pass

    print("\n=== Classification report (RandomForest, baseline) ===")
    # string report for console
    print(classification_report(y_test, y_pred, zero_division=0))

    metrics: dict = {}

    # Accuracy
    acc = accuracy_score(y_test, y_pred)
    metrics["accuracy"] = float(acc)

    # Structured report for per-class stats
    report_dict = classification_report(
        y_test,
        y_pred,
        output_dict=True,
        zero_division=0,
    )
    metrics["classification_report"] = report_dict

    # Per-class metrics where available
    for label in ("0", "1"):
        if label in report_dict:
            metrics[f"precision_{label}"] = float(report_dict[label]["precision"])
            metrics[f"recall_{label}"] = float(report_dict[label]["recall"])
            metrics[f"f1_{label}"] = float(report_dict[label]["f1-score"])

    # ROC-AUC if possible
    if y_prob is not None and len(np.unique(y_test)) == 2:
        try:
            auc = roc_auc_score(y_test, y_prob)
            metrics["roc_auc"] = float(auc)
            print(f"ROC-AUC: {auc:.3f}")
        except ValueError:
            # Not enough class variety
            pass

    # Basic counts
    metrics["n_test"] = int(len(y_test))
    metrics["class_distribution_test"] = {
        str(int(c)): int((y_test == c).sum()) for c in np.unique(y_test)
    }

    return metrics


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a baseline RandomForest model on gold_ml_features."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/warehouse/climate.duckdb"),
        help="Path to the DuckDB warehouse file.",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="gold_ml_features",
        help="Name of the table containing ML features.",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=Path("models/baseline_rf.pkl"),
        help="Where to save the trained model.",
    )
    parser.add_argument(
        "--metrics-path",
        type=Path,
        default=Path("models/baseline_rf_metrics.json"),
        help="Where to save evaluation metrics as JSON.",
    )
    parser.add_argument(
        "--train-fraction",
        type=float,
        default=0.75,
        help="Fraction of data to use for training (rest used for testing).",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=300,
        help="Number of trees in the random forest.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=8,
        help="Maximum depth of each tree (use a small number to avoid overfitting).",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Loading data from {args.db_path} (table={args.table_name})...")
    df = load_ml_table(args.db_path, args.table_name)
    print(f"Loaded {len(df)} rows from {args.table_name}.")

    print("Preparing data (drop NaNs, sort, time-based split)...")
    X_train, X_test, y_train, y_test, df_clean = prepare_data(
        df,
        FEATURE_COLS,
        TARGET_COL,
        train_fraction=args.train_fraction,
    )
    print(f"Training samples: {len(y_train)}, Test samples: {len(y_test)}")

    print("Training RandomForest baseline model...")
    model = train_random_forest(
        X_train,
        y_train,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=args.random_state,
    )

    print("Evaluating model on test split...")
    metrics = evaluate_model(model, X_test, y_test)

    # Ensure output directories exist
    args.model_path.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_path.parent.mkdir(parents=True, exist_ok=True)

    # Save model
    import joblib
    joblib.dump(model, args.model_path)
    print(f"\nSaved model to {args.model_path}")

    # Save metrics JSON (original behavior, plus new fields)
    metrics.update(
        {
            "n_train": int(len(y_train)),
            "feature_columns": FEATURE_COLS,
            "target_column": TARGET_COL,
        }
    )
    args.metrics_path.write_text(json.dumps(metrics, indent=2))
    print(f"Saved metrics to {args.metrics_path}")

    # -------------------------------------------------------------------
    # NEW: Log metrics into DuckDB observability table via MLMetricRecord
    # -------------------------------------------------------------------
    try:
        # Positive-class ratio (for label 1) on test set
        n_test = metrics.get("n_test", len(y_test))
        class_dist = metrics.get("class_distribution_test", {})
        n_pos = class_dist.get("1", 0)
        positive_ratio = (n_pos / n_test) if n_test > 0 else float("nan")

        # Pull per-class metrics (may be absent if label missing)
        precision_0 = metrics.get("precision_0", float("nan"))
        recall_0 = metrics.get("recall_0", float("nan"))
        f1_0 = metrics.get("f1_0", float("nan"))
        precision_1 = metrics.get("precision_1", float("nan"))
        recall_1 = metrics.get("recall_1", float("nan"))
        f1_1 = metrics.get("f1_1", float("nan"))

        accuracy = metrics.get("accuracy", float("nan"))
        roc_auc = metrics.get("roc_auc", float("nan"))

        # Optional linkage to a pipeline run (if Prefect sets this env var)
        pipeline_run_id_env = os.getenv("PIPELINE_RUN_ID")
        pipeline_run_id = int(pipeline_run_id_env) if pipeline_run_id_env else None

        record = MLMetricRecord(
            pipeline_run_id=pipeline_run_id,
            flow_name=DEFAULT_FLOW_NAME,
            run_mode=DEFAULT_RUN_MODE,
            model_name=DEFAULT_MODEL_NAME,
            model_version=DEFAULT_MODEL_VERSION,
            train_size=int(len(y_train)),
            test_size=int(len(y_test)),
            positive_class_ratio=float(positive_ratio),
            accuracy=float(accuracy),
            roc_auc=float(roc_auc),
            precision_0=float(precision_0),
            recall_0=float(recall_0),
            f1_0=float(f1_0),
            precision_1=float(precision_1),
            recall_1=float(recall_1),
            f1_1=float(f1_1),
            created_at=datetime.now(timezone.utc),
        )

        log_ml_metrics(record)
        print("✅ Logged ML metrics to DuckDB (pipeline_ml_metrics).")

    except Exception as e:
        # Observability should never break the training script
        print(f"⚠️ Failed to log ML metrics to DuckDB: {e}")


if __name__ == "__main__":
    main()