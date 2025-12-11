# tests/test_anomaly_layer.py

import pandas as pd


def test_anomaly_tables_exist(con):
    """
    Ensure the anomaly-layer tables exist:
      - anomaly_city_month
      - anomaly_city_events
      - anomaly_city_lags
      - anomaly_city_correlations
    """
    tables = con.execute("SHOW TABLES").df()["name"].tolist()

    expected = {
        "anomaly_city_month",
        "anomaly_city_events",
        "anomaly_city_lags",
        "anomaly_city_correlations",
    }

    missing = expected - set(tables)
    assert not missing, f"Missing anomaly tables: {missing}"


def test_anomaly_city_month_schema(con):
    """
    Basic schema checks for anomaly_city_month:
      - must have (city_id, year, month)
      - year and month should be integer-like
    """
    df = con.execute("SELECT * FROM anomaly_city_month LIMIT 1").df()
    if df.empty:
        # Let the "not empty" test handle emptiness;
        # this test is only about schema.
        return

    required = {"city_id", "year", "month"}
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns in anomaly_city_month: {missing}"

    assert pd.api.types.is_integer_dtype(df["year"]), (
        "Expected 'year' column in anomaly_city_month to be integer-like."
    )
    assert pd.api.types.is_integer_dtype(df["month"]), (
        "Expected 'month' column in anomaly_city_month to be integer-like."
    )


def test_anomaly_city_events_schema(con):
    """
    Basic schema checks for anomaly_city_events:
      - must have (city_id, year, month)
      - must have the event flag column is_event_next_month
    """
    df = con.execute("SELECT * FROM anomaly_city_events LIMIT 1").df()
    if df.empty:
        return

    required = {"city_id", "year", "month", "is_event_next_month"}
    missing = required - set(df.columns)
    assert not missing, f"Missing required columns in anomaly_city_events: {missing}"

    assert pd.api.types.is_integer_dtype(df["year"]), (
        "Expected 'year' column in anomaly_city_events to be integer-like."
    )
    assert pd.api.types.is_integer_dtype(df["month"]), (
        "Expected 'month' column in anomaly_city_events to be integer-like."
    )


def test_anomaly_tables_not_empty(con):
    """
    Sanity check that anomaly tables are non-empty if the pipeline ran.
    """
    counts = {}
    for table in [
        "anomaly_city_month",
        "anomaly_city_events",
        "anomaly_city_lags",
        "anomaly_city_correlations",
    ]:
        n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        counts[table] = n

    # At minimum, month + events should be populated if upstream ran.
    assert counts["anomaly_city_month"] > 0, (
        "anomaly_city_month is empty — check upstream dbt models."
    )
    assert counts["anomaly_city_events"] > 0, (
        "anomaly_city_events is empty — check anomaly event generation."
    )