import subprocess
import sys
from pathlib import Path
import time
import argparse

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = PROJECT_ROOT / "dbt"
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "climate.duckdb"


def run_command(command, cwd=None, description=None):
    print("\n" + "=" * 60)
    if description:
        print(f"▶ {description}")
    print(f"$ {' '.join(command)}")
    print("=" * 60)

    result = subprocess.run(command, cwd=cwd)

    if result.returncode != 0:
        print(f"❌ Command failed: {' '.join(command)}")
        sys.exit(result.returncode)

    print("✅ Success")


def check_db_lock():
    print("\nChecking DuckDB lock status…")

    if not DB_PATH.exists():
        print(f"⚠ Database not found: {DB_PATH}")
        return

    # Give DuckDB / Python a moment to release locks
    time.sleep(1)
    print(f"✅ Using DuckDB at: {DB_PATH}")


# ==========================
# Ingestion steps
# ==========================

def run_ingestion_recent():
    """
    Incremental ingestion: keep current year up to date.
    Uses fetch-daily-weather --mode recent.
    """
    run_command(
        ["uv", "run", "fetch-daily-weather", "--mode", "recent"],
        cwd=str(PROJECT_ROOT),
        description="Ingesting recent daily weather data (year-to-date, all cities)",
    )


def run_ingestion_backfill():
    """
    Historical ingestion: load a full historical window.
    Uses fetch-daily-weather --mode backfill (window from settings.yaml).
    """
    run_command(
        ["uv", "run", "fetch-daily-weather", "--mode", "backfill"],
        cwd=str(PROJECT_ROOT),
        description="Ingesting historical daily weather data (backfill, all cities)",
    )


# ==========================
# dbt, tests, ML, dashboard
# ==========================

def run_dbt_models():
    run_command(
        ["uv", "run", "dbt", "run"],
        cwd=str(DBT_DIR),
        description="Running dbt models (bronze → silver → gold)",
    )


def run_dbt_tests():
    run_command(
        ["uv", "run", "dbt", "test"],
        cwd=str(DBT_DIR),
        description="Running dbt tests",
    )


def run_tests():
    run_command(
        ["uv", "run", "pytest"],
        cwd=str(PROJECT_ROOT),
        description="Running project tests (pytest)",
    )


def run_ml_training():
    run_command(
        ["uv", "run", "climate-train-baseline"],
        cwd=str(PROJECT_ROOT),
        description="Training baseline anomaly model",
    )


def run_inference():
    run_command(
        ["uv", "run", "climate-predict-baseline"],
        cwd=str(PROJECT_ROOT),
        description="Running baseline inference",
    )


def run_streamlit():
    run_command(
        ["uv", "run", "streamlit", "run", "dashboards/streamlit/app.py"],
        cwd=str(PROJECT_ROOT),
        description="Launching Streamlit dashboard",
    )


def main():
    parser = argparse.ArgumentParser(description="Run entire climate pipeline")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help=(
            "Pipeline mode. "
            "'daily' = incremental run using fetch-daily-weather --mode recent; "
            "'backfill' = historical run using fetch-daily-weather --mode backfill "
            "(time window from settings.yaml or CLI overrides in the ingestion step). "
            "Default: daily."
        ),
    )
    parser.add_argument(
        "--with-tests",
        action="store_true",
        help="Run pytest after successful pipeline execution",
    )
    parser.add_argument(
        "--with-dbt-tests",
        action="store_true",
        help="Run dbt tests after models are built",
    )

    args = parser.parse_args()

    print("\n🌍 Starting Climate Pipeline Orchestration\n")
    print(f"Pipeline mode: {args.mode}")

    # 1) Ingestion
    if args.mode == "daily":
        run_ingestion_recent()
    elif args.mode == "backfill":
        run_ingestion_backfill()
    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)

    # 2) Warehouse step (DuckDB via dbt)
    check_db_lock()
    run_dbt_models()

    if args.with_dbt_tests:
        run_dbt_tests()

    # 3) ML training
    run_ml_training()

    # 4) Optional test suite
    if args.with_tests:
        print("\n🧪 Running test suite because --with-tests was provided\n")
        run_tests()

    print("\n✅ Climate pipeline completed successfully\n")


if __name__ == "__main__":
    main()