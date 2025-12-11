from __future__ import annotations

import os
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd


# ======================================================
# Paths / basic helpers
# ======================================================

# This file lives at: src/climate_pipeline/cli/health.py
# __file__.parents: [cli, climate_pipeline, src, <project_root>, ...]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "climate.duckdb"


def _resolve_db_path(db_path: Optional[str | Path] = None) -> Path:
    """
    Resolve the DuckDB path with the following precedence:

      1. Explicit db_path argument
      2. DUCKDB_PATH environment variable
      3. data/warehouse/climate.duckdb under the project root
    """
    if db_path is not None:
        return Path(db_path)

    env_path = os.environ.get("DUCKDB_PATH")
    if env_path:
        return Path(env_path)

    return DEFAULT_DB_PATH


def _classify_freshness(max_date: Optional[date]) -> str:
    """
    Mirror the freshness logic in compute_run_stats:
        <= 1 day   -> "fresh"
        <= 7 days  -> "stale"
        > 7 days   -> "very_stale"
    """
    if max_date is None:
        return "unknown"

    today = datetime.now(timezone.utc).date()
    lag_days = (today - max_date).days

    if lag_days <= 1:
        return "fresh"
    elif lag_days <= 7:
        return "stale"
    else:
        return "very_stale"


# ======================================================
# Ingestion health check
# ======================================================

def _load_ingestion_summary(db_path: Path) -> pd.DataFrame:
    """
    Build a per-city ingestion summary from bronze_daily_weather.

    Columns:
      - city_id
      - city_name
      - country_code
      - first_date
      - last_date
      - n_days
      - freshness_status
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Join seed dim_city (from dbt) with bronze_daily_weather
        df = con.execute(
            """
            SELECT
                b.city_id,
                c.city_name,
                c.country_code,
                MIN(b.date)            AS first_date,
                MAX(b.date)            AS last_date,
                COUNT(*)               AS n_days
            FROM "main"."bronze_daily_weather" b
            LEFT JOIN "main"."dim_city" c USING (city_id)
            GROUP BY b.city_id, c.city_name, c.country_code
            ORDER BY b.city_id
            """
        ).df()
    finally:
        con.close()

    if df.empty:
        return df

    # Add freshness classification
    df["freshness_status"] = df["last_date"].apply(_classify_freshness)

    return df


def check_ingestion_main() -> None:
    """
    Console entrypoint for `climate-check-ingestion`.

    Exit codes:
      0 -> All cities are 'fresh'
      1 -> Some cities are 'stale' but none are 'very_stale'
      2 -> At least one city is 'very_stale'
      3 -> Structural problem (no data / DB missing / query failure)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Check ingestion health from bronze_daily_weather in DuckDB."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Override DuckDB path. Defaults to DUCKDB_PATH env or data/warehouse/climate.duckdb",
    )

    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path)
    print(f"🔍 Checking ingestion health using DuckDB at: {db_path}")

    try:
        df = _load_ingestion_summary(db_path)
    except Exception as exc:
        print(f"❌ Failed to load ingestion summary: {exc}")
        raise SystemExit(3)

    if df.empty:
        print("⚠️  bronze_daily_weather is empty. Run the pipeline first.")
        raise SystemExit(3)

    print("\nPer-city ingestion summary:\n")
    display_df = df.copy()
    display_df["first_date"] = display_df["first_date"].astype(str)
    display_df["last_date"] = display_df["last_date"].astype(str)
    print(display_df.to_string(index=False))

    # Freshness classification counts
    statuses = df["freshness_status"].value_counts().to_dict()
    n_fresh = statuses.get("fresh", 0)
    n_stale = statuses.get("stale", 0)
    n_very_stale = statuses.get("very_stale", 0)
    n_unknown = statuses.get("unknown", 0)

    print("\nFreshness counts:", statuses)

    # Exit-code logic for CI
    if n_very_stale > 0:
        print(
            "\n❌ Ingestion health check FAILED: "
            f"{n_very_stale} city/cities are 'very_stale' (> 7 days behind)."
        )
        raise SystemExit(2)
    elif n_stale > 0:
        print(
            "\n⚠️ Ingestion health check WARNING: "
            f"{n_stale} city/cities are 'stale' (1–7 days behind)."
        )
        raise SystemExit(1)
    elif n_unknown > 0:
        print(
            "\n⚠️ Ingestion health check WARNING: "
            f"{n_unknown} city/cities have 'unknown' freshness."
        )
        # treat as soft warning
        raise SystemExit(1)
    else:
        print("\n✅ All cities are fresh (<= 1 day lag).")
        raise SystemExit(0)


# ======================================================
# ML health check
# ======================================================

def _load_ml_summary(db_path: Path) -> pd.DataFrame:
    """
    Load daily ML summary from pipeline_ml_daily_summary, if present.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # This table is created by dbt observability models
        df = con.execute(
            """
            SELECT
                run_date,
                run_mode,
                n_runs,
                last_run_at,
                last_n_train,
                avg_accuracy,
                avg_roc_auc
            FROM "main"."pipeline_ml_daily_summary"
            ORDER BY run_date DESC, run_mode
            """
        ).df()
    finally:
        con.close()

    return df


def check_ml_main() -> None:
    """
    Console entrypoint for `climate-check-ml`.

    Reads pipeline_ml_daily_summary and evaluates the latest day
    against thresholds.

    Exit codes:
      0 -> Metrics meet or exceed thresholds
      1 -> Metrics below thresholds (but data present)
      3 -> Structural problem (no data / DB missing / query failure)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Check ML training health from pipeline_ml_daily_summary."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Override DuckDB path. Defaults to DUCKDB_PATH env or data/warehouse/climate.duckdb",
    )
    parser.add_argument(
        "--min-accuracy",
        type=float,
        default=float(os.environ.get("CLIMATE_MIN_ACCURACY", 0.80)),
        help="Minimum acceptable accuracy for latest run (default: 0.80 or CLIMATE_MIN_ACCURACY).",
    )
    parser.add_argument(
        "--min-roc-auc",
        type=float,
        default=float(os.environ.get("CLIMATE_MIN_ROC_AUC", 0.60)),
        help="Minimum acceptable ROC-AUC for latest run (default: 0.60 or CLIMATE_MIN_ROC_AUC).",
    )
    parser.add_argument(
        "--min-positive-ratio",
        type=float,
        default=float(os.environ.get("CLIMATE_MIN_POSITIVE_RATIO", 0.01)),
        help="Minimum acceptable positive class ratio in test set (default: 0.01 or CLIMATE_MIN_POSITIVE_RATIO).",
    )

    args = parser.parse_args()
    db_path = _resolve_db_path(args.db_path)

    print(f"🔍 Checking ML health using DuckDB at: {db_path}")
    try:
        df = _load_ml_summary(db_path)
    except Exception as exc:
        print(f"❌ Failed to load ML summary: {exc}")
        raise SystemExit(3)

    if df.empty:
        print(
            "⚠️  pipeline_ml_daily_summary is empty. "
            "Have you run climate-train-baseline / the Prefect flow?"
        )
        raise SystemExit(3)

    print("\nRecent ML daily summary (head):\n")
    print(df.head(10).to_string(index=False))

    latest = df.iloc[0]
    run_date = latest["run_date"]
    run_mode = latest["run_mode"]
    n_runs = latest["n_runs"]
    last_run_at = latest["last_run_at"]
    last_n_train = latest["last_n_train"]
    avg_accuracy = float(latest["avg_accuracy"])
    avg_roc_auc = float(latest["avg_roc_auc"])

    print("\nMost recent ML training day:")
    print(
        f"  Date:       {run_date}\n"
        f"  Mode:       {run_mode}\n"
        f"  n_runs:     {n_runs}\n"
        f"  last_run:   {last_run_at}\n"
        f"  n_train:    {last_n_train}\n"
        f"  accuracy:   {avg_accuracy:.3f}\n"
        f"  roc_auc:    {avg_roc_auc:.3f}"
    )

    # Try to get positive_class_ratio for the same day/run_mode from the base table, if available
    pos_ratio = None
    try:
        # We only need the last record for that day/mode
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT positive_class_ratio
                FROM "main"."pipeline_ml_metrics"
                WHERE DATE(created_at) = ?
                  AND run_mode = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [run_date, run_mode],
            ).fetchone()
        finally:
            con.close()

        if row is not None:
            pos_ratio = float(row[0])
    except Exception as exc:
        print(f"⚠️ Could not retrieve positive_class_ratio from pipeline_ml_metrics: {exc}")

    # Evaluate against thresholds
    failures = []

    if avg_accuracy < args.min_accuracy:
        failures.append(
            f"accuracy {avg_accuracy:.3f} < min_accuracy {args.min_accuracy:.3f}"
        )

    if avg_roc_auc < args.min_roc_auc:
        failures.append(
            f"roc_auc {avg_roc_auc:.3f} < min_roc_auc {args.min_roc_auc:.3f}"
        )

    if pos_ratio is not None and pos_ratio < args.min_positive_ratio:
        failures.append(
            f"positive_class_ratio {pos_ratio:.4f} < min_positive_ratio {args.min_positive_ratio:.4f}"
        )

    if failures:
        print("\n❌ ML health check FAILED:")
        for f in failures:
            print(f"  - {f}")
        raise SystemExit(1)

    print("\n✅ ML metrics meet configured thresholds.")
    raise SystemExit(0)


# ======================================================
# Streamlit dashboard launcher
# ======================================================

def run_streamlit_dashboard_main() -> None:
    """
    Console entrypoint for `climate-dashboard`.

    Runs the Streamlit app defined at dashboards/streamlit/app.py.
    """
    app_path = PROJECT_ROOT / "dashboards" / "streamlit" / "app.py"

    if not app_path.exists():
        print(f"❌ Streamlit app not found at {app_path}")
        raise SystemExit(1)

    print(f"🚀 Launching Streamlit dashboard from {app_path}")
    # Inherit environment; DUCKDB_PATH etc. will still apply.
    subprocess.run(
        ["streamlit", "run", str(app_path)],
        check=True,
    )