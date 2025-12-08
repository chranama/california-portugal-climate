# src/climate_pipeline/dbt_cli.py

from dbt.cli.main import cli


def docs() -> None:
    """
    Serve dbt docs for the climate_pipeline project.

    Usage (from project root):
        uv run climate-dbt-docs
    """
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
    cli(
        [
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ]
    )