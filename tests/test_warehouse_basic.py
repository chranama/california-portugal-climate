# tests/test_warehouse_basic.py

def test_warehouse_has_tables(con):
    tables = con.execute("SHOW TABLES").fetchdf()
    table_names = set(tables["name"].tolist())

    # Adjust this list if names change
    expected = {
        "bronze_daily_weather",
        "silver_daily_weather_features",
        "silver_monthly_climate",
        "gold_city_month_anomalies",
    }

    missing = expected - table_names
    assert not missing, f"Missing expected tables: {missing}"


def test_bronze_daily_weather_not_empty(con):
    count = con.execute("SELECT COUNT(*) AS n FROM bronze_daily_weather").fetchone()[0]
    assert count > 0, "bronze_daily_weather should have at least one row"


def test_silver_monthly_climate_not_empty(con):
    count = con.execute("SELECT COUNT(*) AS n FROM silver_monthly_climate").fetchone()[0]
    assert count > 0, "silver_monthly_climate should have at least one row"