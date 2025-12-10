from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional

import duckdb


# ==========================
# Paths
# ==========================

# This file lives at: src/climate_pipeline/observability/run_logger.py
# __file__.parents: [observability, climate_pipeline, src, <project_root>, ...]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "climate.duckdb"


# ==========================
# Data classes
# ==========================

@dataclass
class PipelineRunRecord:
    """
    Single pipeline run record that will be inserted into pipeline_run_log.
    Mirrors what the dbt view main.pipeline_runs expects.
    """
    flow_name: str
    run_mode: str
    status: str
    started_at: datetime
    finished_at: datetime
    rows_bronze: int
    rows_gold_ml: int
    rows_bronze_delta: int
    rows_gold_ml_delta: int
    bronze_max_date: Optional[date]
    gold_ml_max_date: Optional[date]
    freshness_status: str

@dataclass
class MLMetricRecord:
    """
    Single ML training/eval run metrics.

    This is meant to be logged once per training run, and optionally
    linked back to a pipeline_run_log row via pipeline_run_id.
    """
    # Link back to pipeline run (optional, can be null)
    pipeline_run_id: Optional[int]

    # High-level context
    flow_name: str             # e.g. "daily-climate-pipeline"
    run_mode: str              # e.g. "daily", "backfill"
    model_name: str            # e.g. "baseline_random_forest"
    model_version: str         # e.g. "v1" or a git SHA

    # Dataset sizes
    train_size: int
    test_size: int

    # Class distribution / label stats (for anomaly = 1)
    positive_class_ratio: float   # fraction of positives in test set (0–1)

    # Overall performance
    accuracy: float
    roc_auc: float

    # Per-class metrics (0 = normal, 1 = anomaly)
    precision_0: float
    recall_0: float
    f1_0: float

    precision_1: float
    recall_1: float
    f1_1: float

    # When metrics were computed (UTC)
    created_at: datetime


@dataclass
class PipelineRunStats:
    """
    Computed statistics for the *current* run, before we write the record.
    """
    rows_bronze: int
    rows_gold_ml: int
    rows_bronze_delta: int
    rows_gold_ml_delta: int
    bronze_max_date: Optional[date]
    gold_ml_max_date: Optional[date]
    freshness_status: str


# ==========================
# Helpers
# ==========================

def _get_warehouse_path(warehouse_path: Optional[Path] = None) -> Path:
    """
    Resolve the DuckDB file path, with a sensible default inside data/warehouse.
    """
    if warehouse_path is not None:
        return warehouse_path
    return WAREHOUSE_PATH


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Ensure the observability tables exist.

    IMPORTANT:
    - This function assumes `conn` is already open.
    - It DOES NOT open or close the connection.
    """
    # ---------------------------------
    # 1) Pipeline run log (already used)
    # ---------------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            id BIGINT,
            flow_name TEXT,
            run_mode TEXT,
            status TEXT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            rows_bronze BIGINT,
            rows_gold_ml BIGINT,
            rows_bronze_delta BIGINT,
            rows_gold_ml_delta BIGINT,
            bronze_max_date DATE,
            gold_ml_max_date DATE,
            freshness_status TEXT
        )
        """
    )

    # ---------------------------------
    # 2) ML metrics table (NEW)
    # ---------------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_ml_metrics (
            id BIGINT,
            pipeline_run_id BIGINT,     -- optional FK to pipeline_run_log.id

            flow_name TEXT,
            run_mode TEXT,
            model_name TEXT,
            model_version TEXT,

            train_size BIGINT,
            test_size BIGINT,
            positive_class_ratio DOUBLE,

            accuracy DOUBLE,
            roc_auc DOUBLE,

            precision_0 DOUBLE,
            recall_0 DOUBLE,
            f1_0 DOUBLE,

            precision_1 DOUBLE,
            recall_1 DOUBLE,
            f1_1 DOUBLE,

            created_at TIMESTAMP
        )
        """
    )


def _compute_next_id(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Compute the next ID for pipeline_run_log in a simple, concurrency-safe way
    (enough for local and single-process usage).
    """
    row = conn.execute("SELECT COALESCE(MAX(id) + 1, 1) FROM pipeline_run_log").fetchone()
    return int(row[0])


# ==========================
# Public API
# ==========================

def compute_run_stats(warehouse_path: Optional[Path] = None) -> PipelineRunStats:
    """
    Open a fresh DuckDB connection, compute the current run statistics, then close it.

    This function MUST NOT reuse a closed connection and is safe to call from Prefect.
    """
    db_path = _get_warehouse_path(warehouse_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        _ensure_schema(conn)

        # --- Current row counts ---
        rows_bronze = conn.execute(
            'SELECT COUNT(*) FROM "main"."bronze_daily_weather"'
        ).fetchone()[0]

        rows_gold_ml = conn.execute(
            'SELECT COUNT(*) FROM "main"."gold_ml_features"'
        ).fetchone()[0]

        # --- Max dates for freshness ---

        # bronze: real daily date column
        bronze_max_date = conn.execute(
            'SELECT MAX(date) FROM "main"."bronze_daily_weather"'
        ).fetchone()[0]

        # gold_ml: monthly features → derive a date from (year, month)
        latest_month_row = conn.execute(
            """
            SELECT year, month
            FROM "main"."gold_ml_features"
            ORDER BY year DESC, month DESC
            LIMIT 1
            """
        ).fetchone()

        if latest_month_row is not None:
            latest_year, latest_month = latest_month_row
            gold_ml_max_date = date(int(latest_year), int(latest_month), 1)
        else:
            gold_ml_max_date = None

        # --- Previous successful run (for deltas) ---
        last_success = conn.execute(
            """
            SELECT
                rows_bronze,
                rows_gold_ml
            FROM pipeline_run_log
            WHERE status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

        if last_success is None:
            rows_bronze_delta = rows_bronze
            rows_gold_ml_delta = rows_gold_ml
        else:
            prev_rows_bronze, prev_rows_gold_ml = last_success
            rows_bronze_delta = rows_bronze - int(prev_rows_bronze)
            rows_gold_ml_delta = rows_gold_ml - int(prev_rows_gold_ml)

        # --- Freshness status (based on bronze_daily_weather) ---
        freshness_status = "unknown"
        if bronze_max_date is not None:
            today = datetime.now(timezone.utc).date()
            lag_days = (today - bronze_max_date).days
            if lag_days <= 1:
                freshness_status = "fresh"
            elif lag_days <= 7:
                freshness_status = "stale"
            else:
                freshness_status = "very_stale"

        return PipelineRunStats(
            rows_bronze=int(rows_bronze),
            rows_gold_ml=int(rows_gold_ml),
            rows_bronze_delta=int(rows_bronze_delta),
            rows_gold_ml_delta=int(rows_gold_ml_delta),
            bronze_max_date=bronze_max_date,
            gold_ml_max_date=gold_ml_max_date,
            freshness_status=freshness_status,
        )
    finally:
        conn.close()


def log_pipeline_run(
    record: PipelineRunRecord,
    warehouse_path: Optional[Path] = None,
) -> None:
    """
    Insert a PipelineRunRecord into pipeline_run_log.

    If anything goes wrong here, callers SHOULD catch exceptions so that
    logging failures don't break the main pipeline.
    """
    db_path = _get_warehouse_path(warehouse_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        _ensure_schema(conn)
        next_id = _compute_next_id(conn)

        conn.execute(
            """
            INSERT INTO pipeline_run_log (
                id,
                flow_name,
                run_mode,
                status,
                started_at,
                finished_at,
                rows_bronze,
                rows_gold_ml,
                rows_bronze_delta,
                rows_gold_ml_delta,
                bronze_max_date,
                gold_ml_max_date,
                freshness_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                next_id,
                record.flow_name,
                record.run_mode,
                record.status,
                record.started_at,
                record.finished_at,
                record.rows_bronze,
                record.rows_gold_ml,
                record.rows_bronze_delta,
                record.rows_gold_ml_delta,
                record.bronze_max_date,
                record.gold_ml_max_date,
                record.freshness_status,
            ],
        ).close()
    finally:
        conn.close()

def log_ml_metrics(record: MLMetricRecord, warehouse_path: Optional[Path] = None) -> None:
    """
    Insert a single MLMetricRecord into pipeline_ml_metrics.
    """
    db_path = _get_warehouse_path(warehouse_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        _ensure_schema(conn)

        # Simple id generation: max(id) + 1, or 1 if table empty
        next_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM pipeline_ml_metrics"
        ).fetchone()[0]

        conn.execute(
            """
            INSERT INTO pipeline_ml_metrics (
                id,
                pipeline_run_id,
                flow_name,
                run_mode,
                model_name,
                model_version,
                train_size,
                test_size,
                positive_class_ratio,
                accuracy,
                roc_auc,
                precision_0,
                recall_0,
                f1_0,
                precision_1,
                recall_1,
                f1_1,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                int(next_id),
                record.pipeline_run_id,
                record.flow_name,
                record.run_mode,
                record.model_name,
                record.model_version,
                int(record.train_size),
                int(record.test_size),
                float(record.positive_class_ratio),
                float(record.accuracy),
                float(record.roc_auc),
                float(record.precision_0),
                float(record.recall_0),
                float(record.f1_0),
                float(record.precision_1),
                float(record.recall_1),
                float(record.f1_1),
                record.created_at,
            ],
        )
    finally:
        conn.close()