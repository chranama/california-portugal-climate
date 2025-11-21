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


def run_dbt_models():
    run_command(
        ["uv", "run", "dbt", "run"],
        cwd=str(DBT_DIR),
        description="Running dbt models (bronze → silver → gold)"
    )


def run_dbt_tests():
    run_command(
        ["uv", "run", "dbt", "test"],
        cwd=str(DBT_DIR),
        description="Running dbt tests"
    )

def run_tests():
    run_command(
        ["uv", "run", "pytest"],
        cwd=str(PROJECT_ROOT),
        description="Running project tests (pytest)"
    )

def run_ml_training():
    run_command(
        ["uv", "run", "climate-train-baseline"],
        cwd=str(PROJECT_ROOT),
        description="Training baseline anomaly model"
    )


def run_inference():
    run_command(
        ["uv", "run", "climate-predict-baseline"],
        cwd=str(PROJECT_ROOT),
        description="Running baseline inference"
    )


def run_streamlit():
    run_command(
        ["uv", "run", "streamlit", "run", "dashboards/streamlit/app.py"],
        cwd=str(PROJECT_ROOT),
        description="Launching Streamlit dashboard"
    )


def main():
    parser = argparse.ArgumentParser(description="Run entire climate pipeline")
    parser.add_argument(
        "--with-tests",
        action="store_true",
        help="Run pytest after successful pipeline execution"
    )

    args = parser.parse_args()

    print("\n🌍 Starting Climate Pipeline Orchestration\n")

    check_db_lock()
    run_dbt_models()
    run_ml_training()

    if args.with_tests:
        print("\n🧪 Running test suite because --with-tests was provided\n")
        run_tests()

    print("\n✅ Climate pipeline completed successfully\n")


if __name__ == "__main__":
    main()