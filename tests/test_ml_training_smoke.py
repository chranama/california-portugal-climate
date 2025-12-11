# tests/test_ml_training_smoke.py

from __future__ import annotations

import sys

import duckdb
import pytest

from climate_pipeline.ml.train import main as train_main


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """
    Helper to check whether a table with the given name exists
    in the main schema.
    """
    df = con.execute("SHOW TABLES").df()
    names = df["name"].tolist()
    return name in names


def test_climate_train_baseline_logs_metrics(warehouse_path):
    """
    Smoke test:

    - Ensures ml_features exists (otherwise skips).
    - Runs the training entrypoint once.
    - Asserts that pipeline_ml_metrics has at least one row afterwards.
    """
    db_path = warehouse_path

    if not db_path.exists():
        pytest.skip(f"DuckDB file not found at {db_path}; run dbt build first.")

    # Pre-flight: ensure the ML features table exists
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(con, "ml_features"):
            pytest.skip(
                "Table 'ml_features' does not exist. "
                "Run dbt models before executing this test."
            )
    finally:
        con.close()

    # ------------------------------------------------------------------
    # Run the training script's main() once
    # ------------------------------------------------------------------
    # We call the module's main() directly to avoid spawning a subprocess.
    old_argv = sys.argv
    try:
        sys.argv = [
            "climate-train-baseline",
            "--db-path",
            str(db_path),
            "--table-name",
            "ml_features",
        ]
        train_main()
    finally:
        sys.argv = old_argv

    # ------------------------------------------------------------------
    # Verify that pipeline_ml_metrics has at least one row
    # ------------------------------------------------------------------
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(con, "pipeline_ml_metrics"):
            pytest.fail(
                "pipeline_ml_metrics table was not created by training script."
            )

        n_rows = con.execute(
            'SELECT COUNT(*) FROM "main"."pipeline_ml_metrics"'
        ).fetchone()[0]
    finally:
        con.close()

    assert n_rows > 0, (
        "Expected at least one row in pipeline_ml_metrics after training, "
        f"but found {n_rows}."
    )