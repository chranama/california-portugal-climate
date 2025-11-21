# tests/test_gold_schema.py

def get_columns(con, table_name: str):
    df = con.execute(f"PRAGMA table_info({table_name})").fetchdf()
    return set(df["name"].tolist())


# tests/test_gold_schema.py

def test_gold_anomalies_schema(con):
    cols = get_columns(con, "gold_city_month_anomalies")

    expected = {
        "city_id",
        "city_name",
        "year",
        "month",
        "anomaly_tmean_c",
        # "zscore_tmean_c",  # optional for now
        "is_positive_temp_anomaly",
        "is_negative_temp_anomaly",
    }

    missing = expected - cols
    assert not missing, f"Missing columns in gold_city_month_anomalies: {missing}"


def test_gold_anomalies_no_null_city_or_date(con):
    row = con.execute("""
        SELECT
            SUM(CASE WHEN city_id IS NULL THEN 1 ELSE 0 END) AS null_city_id,
            SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
            SUM(CASE WHEN month IS NULL THEN 1 ELSE 0 END) AS null_month
        FROM gold_city_month_anomalies
    """).fetchone()

    null_city_id, null_year, null_month = row
    assert null_city_id == 0
    assert null_year == 0
    assert null_month == 0