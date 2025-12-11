# tests/test_landing_layer.py

import duckdb
import pandas as pd
import pytest
from pathlib import Path

from climate_pipeline.utils.get_paths import get_duckdb_path


@pytest.fixture
def con():
    """Open a read-only DuckDB connection for tests."""
    db_path = get_duckdb_path()
    assert db_path.exists(), f"DuckDB file not found at {db_path}"
    con = duckdb.connect(str(db_path), read_only=True)
    yield con
    con.close()


def test_landing_table_exists(con):
    """Ensure landing_daily_weather exists in DuckDB."""
    tables = con.execute("SHOW TABLES").df()["name"].tolist()
    assert "landing_daily_weather" in tables, \
        "landing_daily_weather table is missing — did dbt run?"


def test_landing_basic_columns(con):
    """Check presence of foundational columns."""
    df = con.execute(
        'SELECT * FROM landing_daily_weather LIMIT 1'
    ).df()

    # Landing layer MUST have these
    required = {"city_id", "date"}
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns: {missing}"

    # Check that at least one weather variable exists
    weather_cols = {
        c for c in df.columns
        if c not in ["city_id", "date"]
    }
    assert weather_cols, \
        "landing_daily_weather contains no weather columns — unexpected."


def test_landing_date_type(con):
    """Ensure the date column is returned as datetime/date."""
    df = con.execute(
        'SELECT date FROM landing_daily_weather LIMIT 10'
    ).df()

    assert not df.empty, "landing_daily_weather unexpectedly empty."

    # Pandas represents DuckDB DATE as datetime64[ns] (this is fine)
    assert pd.api.types.is_datetime64_any_dtype(df["date"]), \
        "Expected 'date' column to be datetime-like."


def test_landing_not_empty(con):
    """Sanity check: landing layer should not be empty if pipeline ran."""
    n = con.execute(
        'SELECT COUNT(*) FROM landing_daily_weather'
    ).fetchone()[0]

    assert n > 0, "landing_daily_weather is empty — ingestion may not have run."