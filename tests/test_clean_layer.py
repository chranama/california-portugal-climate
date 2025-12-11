# tests/test_clean_layer.py

import pandas as pd


def test_clean_tables_exist(con):
    """
    Ensure the clean-layer tables exist:
      - clean_daily_weather_features
      - clean_monthly_climate
    """
    tables = con.execute("SHOW TABLES").df()["name"].tolist()

    assert "clean_daily_weather_features" in tables, (
        "clean_daily_weather_features table is missing — did dbt run?"
    )
    assert "clean_monthly_climate" in tables, (
        "clean_monthly_climate table is missing — did dbt run?"
    )


def test_clean_daily_basic_schema(con):
    """
    Check some foundational columns on the daily clean model.
    We keep this intentionally minimal but meaningful.
    """
    df = con.execute(
        'SELECT * FROM clean_daily_weather_features LIMIT 1'
    ).df()

    # Required keys we expect to exist
    required = {"city_id", "date"}
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns in clean_daily_weather_features: {missing}"

    # date should be datetime-like (DuckDB DATE → pandas datetime64)
    assert pd.api.types.is_datetime64_any_dtype(df["date"]), (
        "Expected 'date' column in clean_daily_weather_features to be datetime-like."
    )


def test_clean_monthly_basic_schema(con):
    """
    Check some foundational columns on the monthly clean model.
    """
    df = con.execute(
        'SELECT * FROM clean_monthly_climate LIMIT 1'
    ).df()

    required = {"city_id", "year", "month"}
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns in clean_monthly_climate: {missing}"

    # Year and month should be integer-like
    assert pd.api.types.is_integer_dtype(df["year"]), (
        "Expected 'year' column in clean_monthly_climate to be integer-like."
    )
    assert pd.api.types.is_integer_dtype(df["month"]), (
        "Expected 'month' column in clean_monthly_climate to be integer-like."
    )


def test_clean_tables_not_empty(con):
    """
    Sanity check that clean tables are non-empty if the pipeline ran.
    """
    daily_count = con.execute(
        'SELECT COUNT(*) FROM clean_daily_weather_features'
    ).fetchone()[0]
    monthly_count = con.execute(
        'SELECT COUNT(*) FROM clean_monthly_climate'
    ).fetchone()[0]

    assert daily_count > 0, "clean_daily_weather_features is empty — upstream models may not have run."
    assert monthly_count > 0, "clean_monthly_climate is empty — upstream models may not have run."