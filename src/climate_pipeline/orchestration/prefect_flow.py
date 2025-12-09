from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict

from prefect import flow, task, get_run_logger

from climate_pipeline.observability.run_logger import (
    PipelineRunRecord,
    log_pipeline_run,
    compute_run_stats,
)

# ==========================
# Paths
# ==========================

# This file lives at: src/climate_pipeline/orchestration/prefect_flow.py
# __file__.parents: [orchestration, climate_pipeline, src, <project_root>, ...]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = PROJECT_ROOT / "dbt"
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
WAREHOUSE_PATH = DATA_DIR / "warehouse" / "climate.duckdb"


# ==========================
# Subprocess helpers
# ==========================

def build_subprocess_env(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Build an environment dict for subprocesses that ensures
    all critical path-related env vars are absolute and consistent,
    regardless of how .env was defined.
    """
    env = os.environ.copy()

    # Core environment
    env.setdefault("CLIMATE_ENV", "local")

    # Force absolute paths for data, logs, and DuckDB
    env["CLIMATE_DATA_ROOT"] = str(DATA_DIR)
    env["CLIMATE_LOG_ROOT"] = str(LOGS_DIR)
    env["DUCKDB_PATH"] = str(WAREHOUSE_PATH)

    # Ensure dbt always finds the correct profiles directory
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)

    # Open-Meteo defaults if not already set
    env.setdefault(
        "OPEN_METEO_GEOCODING_BASE_URL",
        "https://geocoding-api.open-meteo.com/v1/search",
    )
    env.setdefault(
        "OPEN_METEO_HISTORICAL_BASE_URL",
        "https://archive-api.open-meteo.com/v1/archive",
    )

    if extra:
        env.update(extra)

    return env


def _run_command(
    command: list[str],
    cwd: Optional[Path] = None,
    description: str = "",
    extra_env: Optional[Dict[str, str]] = None,
) -> None:
    """
    Helper to run a subprocess command with logging and a controlled environment.
    Raises on non-zero exit so Prefect can mark the task as failed.
    """
    logger = get_run_logger()
    cwd_path = cwd if cwd is not None else PROJECT_ROOT
    cwd_str = str(cwd_path)

    logger.info("============================================================")
    if description:
        logger.info("▶ %s", description)
    logger.info("$ %s (cwd=%s)", " ".join(command), cwd_str)
    logger.info("============================================================")

    env = build_subprocess_env(extra_env)

    try:
        result = subprocess.run(
            command,
            cwd=cwd_str,
            env=env,
            check=True,
        )
        logger.info("✅ Command succeeded with return code %s", result.returncode)
    except subprocess.CalledProcessError as exc:
        logger.error("❌ Command failed with return code %s", exc.returncode)
        logger.error("Failed command: %s", " ".join(command))
        raise


# ==========================
# Tasks: ingestion
# ==========================

@task(retries=3, retry_delay_seconds=60)
def ingest_recent() -> None:
    """
    Incremental / daily ingestion:
    Uses fetch-daily-weather --mode recent to refresh year-to-date data.
    """
    _run_command(
        ["uv", "run", "fetch-daily-weather", "--mode", "recent"],
        cwd=PROJECT_ROOT,
        description="Ingesting recent daily weather data (year-to-date, all cities)",
    )


@task(retries=3, retry_delay_seconds=60)
def ingest_backfill(start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    """
    Historical ingestion:
    Uses fetch-daily-weather --mode backfill.

    If start_date/end_date are provided (YYYY-MM-DD), they override the
    settings.yaml time_window; otherwise settings.yaml defines the range.
    """
    cmd = ["uv", "run", "fetch-daily-weather", "--mode", "backfill"]

    if start_date is not None:
        cmd.extend(["--start-date", start_date])
    if end_date is not None:
        cmd.extend(["--end-date", end_date])

    _run_command(
        cmd,
        cwd=PROJECT_ROOT,
        description="Ingesting historical daily weather data (backfill, all cities)",
    )


# ==========================
# Tasks: dbt
# ==========================

@task(retries=2, retry_delay_seconds=60)
def run_dbt_build() -> None:
    """
    Run dbt models (bronze → silver → gold).
    Uses the dbt project in the ./dbt directory.
    """
    _run_command(
        ["uv", "run", "dbt", "build", "--project-dir", ".", "--profiles-dir", "."],
        cwd=DBT_DIR,
        description="Running dbt build (incremental bronze + silver + gold models)",
    )


@task(retries=2, retry_delay_seconds=60)
def run_dbt_tests() -> None:
    """
    Run dbt test against the current warehouse state.
    """
    _run_command(
        ["uv", "run", "dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
        cwd=DBT_DIR,
        description="Running dbt tests (data quality checks)",
    )


# ==========================
# Tasks: ML + tests
# ==========================

@task(retries=1, retry_delay_seconds=60)
def run_ml_training() -> None:
    """
    Train or refresh the baseline anomaly model.
    """
    _run_command(
        ["uv", "run", "climate-train-baseline"],
        cwd=PROJECT_ROOT,
        description="Training baseline anomaly model",
    )


@task(retries=1, retry_delay_seconds=60)
def run_pytests() -> None:
    """
    Run the project test suite (pytest).
    """
    _run_command(
        ["uv", "run", "pytest"],
        cwd=PROJECT_ROOT,
        description="Running project tests (pytest)",
    )


# ==========================
# Task: run logging to DuckDB
# ==========================

@task
def log_run_to_duckdb(flow_name: str, run_mode: str, started_at: datetime, finished_at: datetime) -> None:
    """
    Prefect task to record a pipeline run into DuckDB, including:
      - Row counts
      - Row deltas vs previous success
      - Freshness dates
      - Freshness status

    Logging is *best effort*: if DuckDB or the warehouse is unavailable,
    this task logs an error but does not fail the entire flow.
    """
    logger = get_run_logger()

    # 1) Compute stats (best effort)
    try:
        stats = compute_run_stats()
    except Exception as exc:  # duckdb errors, file issues, etc.
        logger.error("❌ Failed to compute run stats for logging: %s", exc)
        return

    # 2) Build record
    record = PipelineRunRecord(
        flow_name=flow_name,
        run_mode=run_mode,
        status="success",  # later we can propagate Prefect failure status
        started_at=started_at,
        finished_at=finished_at,
        rows_bronze=stats.rows_bronze,
        rows_gold_ml=stats.rows_gold_ml,
        rows_bronze_delta=stats.rows_bronze_delta,
        rows_gold_ml_delta=stats.rows_gold_ml_delta,
        bronze_max_date=stats.bronze_max_date,
        gold_ml_max_date=stats.gold_ml_max_date,
        freshness_status=stats.freshness_status,
    )

    # 3) Write record (also best effort)
    try:
        log_pipeline_run(record)
    except Exception as exc:
        logger.error("❌ Failed to log pipeline run to DuckDB: %s", exc)


# ==========================
# Flows
# ==========================

@flow(name="daily-climate-pipeline")
def daily_climate_flow(
    with_dbt_tests: bool = False,
    with_tests: bool = False,
) -> None:
    """
    Daily / incremental pipeline flow.

    Steps:
      1. Incremental ingestion (recent mode)
      2. dbt build (incremental bronze + downstream models)
      3. Optional dbt tests
      4. ML training
      5. Optional pytest
      6. Log run metadata to DuckDB
    """
    logger = get_run_logger()
    logger.info("🌍 Starting Prefect daily_climate_flow")

    started_at = datetime.now(timezone.utc)

    # 1) Ingestion (recent)
    ingest_recent()

    # 2) Transformations via dbt
    run_dbt_build()

    # 3) Optional dbt tests
    if with_dbt_tests:
        logger.info("🧪 Running dbt tests because with_dbt_tests=True")
        run_dbt_tests()

    # 4) ML training
    run_ml_training()

    # 5) Optional pytest
    if with_tests:
        logger.info("🧪 Running pytest because with_tests=True")
        run_pytests()

    finished_at = datetime.now(timezone.utc)

    # 6) Log run metadata into DuckDB (best effort)
    log_run_to_duckdb(
        flow_name="daily-climate-pipeline",
        run_mode="daily",
        started_at=started_at,
        finished_at=finished_at,
    )

    logger.info("✅ daily_climate_flow completed successfully")


@flow(name="backfill-climate-pipeline")
def backfill_climate_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    with_dbt_tests: bool = False,
    with_tests: bool = False,
) -> None:
    """
    Historical / backfill pipeline flow.

    Parameters:
      start_date, end_date (optional, YYYY-MM-DD):
        If provided, they are passed through to fetch-daily-weather;
        otherwise the time_window in settings.yaml is used.

    Steps:
      1. Backfill ingestion
      2. dbt build
      3. Optional dbt tests
      4. ML training
      5. Optional pytest
      6. Log run metadata to DuckDB
    """
    logger = get_run_logger()
    logger.info(
        "🌍 Starting Prefect backfill_climate_flow (start_date=%s, end_date=%s)",
        start_date,
        end_date,
    )

    started_at = datetime.now(timezone.utc)

    # 1) Ingestion (backfill)
    ingest_backfill(start_date=start_date, end_date=end_date)

    # 2) Transformations via dbt
    run_dbt_build()

    # 3) Optional dbt tests
    if with_dbt_tests:
        logger.info("🧪 Running dbt tests because with_dbt_tests=True")
        run_dbt_tests()

    # 4) ML training
    run_ml_training()

    # 5) Optional pytest
    if with_tests:
        logger.info("🧪 Running pytest because with_tests=True")
        run_pytests()

    finished_at = datetime.now(timezone.utc)

    # 6) Log run metadata into DuckDB (best effort)
    log_run_to_duckdb(
        flow_name="backfill-climate-pipeline",
        run_mode="backfill",
        started_at=started_at,
        finished_at=finished_at,
    )

    logger.info("✅ backfill_climate_flow completed successfully")


# ==========================
# Local CLI usage
# ==========================

if __name__ == "__main__":
    """
    Allow running flows directly via:
        uv run python -m climate_pipeline.orchestration.prefect_flow daily
        uv run python -m climate_pipeline.orchestration.prefect_flow backfill
    """
    import argparse

    parser = argparse.ArgumentParser(description="Prefect flows for climate pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # daily flow
    daily_parser = subparsers.add_parser("daily", help="Run daily_climate_flow")
    daily_parser.add_argument("--with-dbt-tests", action="store_true")
    daily_parser.add_argument("--with-tests", action="store_true")

    # backfill flow
    backfill_parser = subparsers.add_parser("backfill", help="Run backfill_climate_flow")
    backfill_parser.add_argument("--start-date", type=str, default=None)
    backfill_parser.add_argument("--end-date", type=str, default=None)
    backfill_parser.add_argument("--with-dbt-tests", action="store_true")
    backfill_parser.add_argument("--with-tests", action="store_true")

    args = parser.parse_args()

    if args.command == "daily":
        daily_climate_flow(
            with_dbt_tests=args.with_dbt_tests,
            with_tests=args.with_tests,
        )
    elif args.command == "backfill":
        backfill_climate_flow(
            start_date=args.start_date,
            end_date=args.end_date,
            with_dbt_tests=args.with_dbt_tests,
            with_tests=args.with_tests,
        )
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)