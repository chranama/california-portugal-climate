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
    Ensure the pipeline_run_log table exists.

    IMPORTANT:
    - This function assumes `conn` is already open.
    - It DOES NOT open or close the connection.
    """
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