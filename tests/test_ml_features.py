# tests/test_ml_features.py

import pandas as pd

# Keep these in sync with src/climate_pipeline/ml/train.py
TARGET_COL = "is_event_next_month"

FEATURE_COLS = [
    "anomaly_tmean_c",
    "roll_mean_3",
    "roll_mean_6",
    "roll_std_3",
    "roll_std_6",
    "delta_1m",
    "delta_3m",
    "max_lagged_corr",
    "lead_lag_months",
    "sin_month",
    "cos_month",
]


def test_ml_features_table_exists(con):
    """
    Ensure the ML feature-store table exists: ml_features.
    """
    tables = con.execute("SHOW TABLES").df()["name"].tolist()
    assert "ml_features" in tables, "Expected table 'ml_features' to exist."


def test_ml_features_basic_schema(con):
    """
    Check that ml_features has:
      - key columns: city_id, year, month
      - target column: is_event_next_month
      - all expected feature columns (from train.py)
    """
    df = con.execute('SELECT * FROM "ml_features" LIMIT 1').df()
    if df.empty:
        # Let the "not empty" test assert on emptiness; this one is schema-focused.
        return

    required = {"city_id", "year", "month", TARGET_COL}
    missing_required = required - set(df.columns)
    assert not missing_required, (
        f"Missing required columns in ml_features: {missing_required}"
    )

    missing_features = set(FEATURE_COLS) - set(df.columns)
    assert not missing_features, (
        f"Missing expected feature columns in ml_features: {missing_features}"
    )

    # Basic type sanity for year/month
    assert pd.api.types.is_integer_dtype(df["year"]), (
        "Expected 'year' in ml_features to be integer-like."
    )
    assert pd.api.types.is_integer_dtype(df["month"]), (
        "Expected 'month' in ml_features to be integer-like."
    )


def test_ml_features_not_empty(con):
    """
    Ensure ml_features has at least one row if pipeline ran.
    """
    n = con.execute('SELECT COUNT(*) FROM "ml_features"').fetchone()[0]
    assert n > 0, "ml_features is empty — check upstream dbt models."


def test_target_is_binary(con):
    """
    Sanity check: target column should be binary (0/1) if populated.
    """
    df = con.execute(
        f'SELECT DISTINCT {TARGET_COL} AS target FROM "ml_features" '
        f'WHERE {TARGET_COL} IS NOT NULL'
    ).df()

    # If no targets yet, skip this test softly
    if df.empty:
        return

    unique_vals = set(df["target"].dropna().tolist())
    assert unique_vals.issubset({0, 1}), (
        f"{TARGET_COL} should be binary 0/1. Found values: {unique_vals}"
    )


def test_no_nans_in_features_where_target_present(con):
    """
    Ensure there are no NaNs in feature columns on rows where the target is non-null.
    This mirrors the training-time cleaning logic.
    """
    df = con.execute(
        f'''
        SELECT *
        FROM "ml_features"
        WHERE {TARGET_COL} IS NOT NULL
        '''
    ).df()

    if df.empty:
        return

    # Restrict to feature columns that actually exist
    available_features = [c for c in FEATURE_COLS if c in df.columns]
    feature_df = df[available_features]

    n_nans = feature_df.isna().sum().sum()
    assert n_nans == 0, (
        f"Found {n_nans} NaNs in feature columns where {TARGET_COL} is non-null."
    )