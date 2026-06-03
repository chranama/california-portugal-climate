# src/climate_pipeline/dbt_cli.py

import os

from dbt.cli.main import cli

from climate_pipeline.observability.run_logger import ensure_observability_schema
from climate_pipeline.utils.get_paths import PROJECT_ROOT, get_duckdb_path


def _configure_dbt_environment() -> None:
    os.environ.setdefault("DUCKDB_PATH", str(get_duckdb_path()))
    os.environ.setdefault("DBT_PROFILES_DIR", str(PROJECT_ROOT / "dbt"))


def docs() -> None:
    """
    Serve dbt docs for the climate_pipeline project.

    Usage (from project root):
        uv run climate-dbt-docs
    """
    _configure_dbt_environment()
    cli(
        [
            "docs",
            "serve",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ]
    )


def build() -> None:
    """
    Run `dbt build` for convenience.

    Usage:
        uv run climate-dbt-build
    """
    _configure_dbt_environment()
    ensure_observability_schema()
    cli(
        [
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ]
    )
