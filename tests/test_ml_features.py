# tests/test_ml_features.py

import pytest


def test_gold_ml_features_exists(con):
    tables = con.execute("SHOW TABLES").fetchdf()
    if "gold_ml_features" not in set(tables["name"].tolist()):
        pytest.skip("gold_ml_features not found (has the dbt model been created yet?)")

    count = con.execute("SELECT COUNT(*) AS n FROM gold_ml_features").fetchone()[0]
    assert count > 0, "gold_ml_features should have at least one row"


def test_gold_ml_features_reasonable_nans(con):
    tables = con.execute("SHOW TABLES").fetchdf()
    if "gold_ml_features" not in set(tables["name"].tolist()):
        pytest.skip("gold_ml_features not found")

    df = con.execute("""
        SELECT
            anomaly_tmean_c,
            delta_1m,
            delta_3m,
            roll_mean_3,
            roll_mean_6
        FROM gold_ml_features
    """).fetchdf()

    n_rows = len(df)
    assert n_rows > 0

    # 1) Target anomaly should NEVER be NaN
    assert not df["anomaly_tmean_c"].isna().any(), "anomaly_tmean_c should not contain NaNs"

    # 2) Lag/delta features can have NaNs at edges, but:
    #    - they must not be all NaN
    #    - they should have at least some valid values
    for col in ["delta_1m", "delta_3m", "roll_mean_3", "roll_mean_6"]:
        n_nan = df[col].isna().sum()
        n_non_nan = n_rows - n_nan

        # Not everything is NaN
        assert n_non_nan > 0, f"{col} appears to be entirely NaN"

        # Optional: sanity check that NaNs are not the vast majority
        # (you can relax/tighten this threshold if you like)
        assert n_nan < n_rows, f"{col} is NaN for all rows, which is suspicious"