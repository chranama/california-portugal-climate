"""
Inference script for the baseline RandomForest anomaly model.

Reads features from DuckDB (gold_ml_features), loads the trained model,
and writes predictions back to DuckDB and to a CSV file.

Usage (from project root):

  uv run climate-predict-baseline

or with custom options:

  uv run climate-predict-baseline \
    --db-path data/warehouse/climate.duckdb \
    --table-name gold_ml_features \
    --model-path models/baseline_rf.pkl \
    --output-table gold_ml_predictions \
    --output-csv data/mart/predictions/baseline_rf_predictions.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import duckdb
import joblib
import numpy as np
import pandas as pd


# Keep these in sync with train.py
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned feature dataframe with NaN rows removed."""
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"Missing expected feature columns: {missing}")

    # Drop rows with NaNs in any feature column (same as training)
    df_clean = df.dropna(subset=FEATURE_COLS).copy()
    return df_clean


def load_model(model_path: Path):
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    return joblib.load(model_path)


def predict_probabilities(model, X: np.ndarray) -> np.ndarray:
    """Get probability of event class (1)."""
    if hasattr(model, "predict_proba"):
        return model.predict_proba(X)[:, 1]
    elif hasattr(model, "decision_function"):
        scores = model.decision_function(X)
        # Squash to (0,1) via logistic function
        return 1.0 / (1.0 + np.exp(-scores))
    else:
        # Fall back to hard predictions; treat as probabilities 0/1
        return model.predict(X).astype(float)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run inference with the baseline RandomForest anomaly model."
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
        help="Path to the trained model file.",
    )
    parser.add_argument(
        "--output-table",
        type=str,
        default="gold_ml_predictions",
        help="Name of the DuckDB table to create/replace with predictions.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("data/mart/predictions/baseline_rf_predictions.csv"),
        help="Path to write predictions as CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # ------------------------------------------------------------------ #
    # Load data from DuckDB
    # ------------------------------------------------------------------ #
    if not args.db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {args.db_path}")

    print(f"Connecting to DuckDB at {args.db_path}")
    con = duckdb.connect(str(args.db_path))

    print(f"Loading features from table '{args.table_name}'...")
    df = con.execute(f"SELECT * FROM {args.table_name}").df()
    if df.empty:
        raise ValueError(f"Table {args.table_name} is empty; nothing to score.")

    # Keep identifier columns for output if present
    id_cols = [c for c in ["city_id", "city_name", "country_code", "year", "month"]
               if c in df.columns]

    df_clean = load_features(df)
    print(f"Rows before cleaning: {len(df)}, after dropping NaN features: {len(df_clean)}")

    X = df_clean[FEATURE_COLS].to_numpy()

    # ------------------------------------------------------------------ #
    # Load model and run predictions
    # ------------------------------------------------------------------ #
    print(f"Loading model from {args.model_path}")
    model = load_model(args.model_path)

    print("Running predictions...")
    prob_event = predict_probabilities(model, X)
    pred_event = (prob_event >= 0.5).astype(int)

    # Build output dataframe
    output_cols = {}
    for c in id_cols:
        output_cols[c] = df_clean[c].values

    # You can add any features you want to expose here as context
    output_cols["anomaly_tmean_c"] = df_clean["anomaly_tmean_c"].values
    output_cols["pred_event_next_month"] = pred_event
    output_cols["prob_event_next_month"] = prob_event

    df_pred = pd.DataFrame(output_cols)

    # ------------------------------------------------------------------ #
    # Write outputs
    # ------------------------------------------------------------------ #
    # 1) Write to DuckDB table
    print(f"Writing predictions to DuckDB table '{args.output_table}'...")
    con.register("preds_df", df_pred)
    con.execute(f"CREATE OR REPLACE TABLE {args.output_table} AS SELECT * FROM preds_df")
    con.unregister("preds_df")
    con.close()

    # 2) Write to CSV
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    df_pred.to_csv(args.output_csv, index=False)
    print(f"Saved predictions CSV to {args.output_csv}")

    print("Inference completed successfully.")


if __name__ == "__main__":
    main()