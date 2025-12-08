from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import duckdb
import os


@dataclass
class PipelineRunRecord:
    flow_name: str
    run_mode: str          # e.g. "daily" or "backfill"
    status: str            # e.g. "success"
    started_at: datetime
    finished_at: datetime
    rows_bronze: Optional[int] = None
    rows_gold_ml: Optional[int] = None


def _get_duckdb_path() -> Path:
    """
    Resolve the DuckDB path from DUCKDB_PATH env var (if set),
    otherwise fall back to the default project-relative path.
    """
    env_path = os.getenv("DUCKDB_PATH")
    if env_path:
        return Path(env_path).resolve()
    return Path("data/warehouse/climate.duckdb").resolve()


def _ensure_pipeline_runs_table(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create the pipeline_runs table if it does not already exist.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            flow_name VARCHAR,
            run_mode VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            rows_bronze BIGINT,
            rows_gold_ml BIGINT
        )
        """
    )


def log_pipeline_run(record: PipelineRunRecord) -> None:
    """
    Append a record to the pipeline_runs table in DuckDB.

    If the table does not exist, it will be created.
    """
    db_path = _get_duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Use a context manager so the connection is guaranteed to be open
    # during the INSERT and closed afterwards.
    with duckdb.connect(str(db_path)) as conn:
        _ensure_pipeline_runs_table(conn)

        conn.execute(
            """
            INSERT INTO pipeline_runs (
                flow_name,
                run_mode,
                status,
                started_at,
                finished_at,
                rows_bronze,
                rows_gold_ml
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.flow_name,
                record.run_mode,
                record.status,
                record.started_at,
                record.finished_at,
                record.rows_bronze,
                record.rows_gold_ml,
            ],
        )


def compute_row_counts() -> Tuple[Optional[int], Optional[int]]:
    """
    Best-effort computation of key table row counts.

    Returns (rows_bronze, rows_gold_ml).
    If a table is missing, returns None for that entry.
    """
    db_path = _get_duckdb_path()
    if not db_path.exists():
        return None, None

    rows_bronze: Optional[int] = None
    rows_gold_ml: Optional[int] = None

    with duckdb.connect(str(db_path)) as conn:
        try:
            rows_bronze = conn.execute(
                "SELECT COUNT(*) FROM bronze_daily_weather"
            ).fetchone()[0]
        except duckdb.Error:
            rows_bronze = None

        try:
            rows_gold_ml = conn.execute(
                "SELECT COUNT(*) FROM gold_ml_features"
            ).fetchone()[0]
        except duckdb.Error:
            rows_gold_ml = None

    return rows_bronze, rows_gold_ml