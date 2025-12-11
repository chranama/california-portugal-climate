# tests/test_observability_models.py

import pandas as pd


def _table_or_view_exists(con, name: str) -> bool:
    """
    Helper to check whether a table or view with the given name exists
    in the main schema.
    """
    df = con.execute("SHOW TABLES").df()
    # DuckDB's SHOW TABLES includes a 'name' column
    names = df["name"].tolist()
    return name in names


# -------------------------------------------------------------------
# pipeline_runs view
# -------------------------------------------------------------------

def test_pipeline_runs_view_exists(con):
    """
    Ensure the pipeline_runs view exists (dbt observability).
    """
    assert _table_or_view_exists(con, "pipeline_runs"), (
        "Expected view/table 'pipeline_runs' to exist. "
        "Run dbt observability models if this fails."
    )


def test_pipeline_runs_basic_columns(con):
    """
    Check that pipeline_runs exposes the expected core columns.
    """
    if not _table_or_view_exists(con, "pipeline_runs"):
        # Fail loudly; this is a structural error.
        raise AssertionError("pipeline_runs does not exist.")

    df = con.execute('SELECT * FROM "pipeline_runs" LIMIT 1').df()

    # Even if empty, we can inspect the columns
    expected = {
        "id",
        "flow_name",
        "run_mode",
        "status",
        "started_at",
        "finished_at",
        "rows_bronze",
        "rows_gold_ml",
        "rows_bronze_delta",
        "rows_gold_ml_delta",
        "bronze_max_date",
        "gold_ml_max_date",
        "freshness_status",
    }
    missing = expected - set(df.columns)
    assert not missing, (
        f"pipeline_runs is missing expected columns: {missing}"
    )


# -------------------------------------------------------------------
# pipeline_run_daily_summary
# -------------------------------------------------------------------

def test_pipeline_run_daily_summary_exists(con):
    """
    Ensure the daily pipeline run aggregation exists.
    """
    assert _table_or_view_exists(con, "pipeline_run_daily_summary"), (
        "Expected table 'pipeline_run_daily_summary' to exist. "
        "Run dbt observability models if this fails."
    )


def test_pipeline_run_daily_summary_columns(con):
    """
    Check that pipeline_run_daily_summary has key summary fields.
    """
    df = con.execute(
        'SELECT * FROM "pipeline_run_daily_summary" LIMIT 1'
    ).df()

    expected_subset = {
        "run_date",
        "flow_name",
        "run_mode",
        "n_runs",
        "last_run_at",
        "rows_bronze_max",
        "rows_gold_ml_max",
        "rows_bronze_delta_max",
        "rows_gold_ml_delta_max",
    }
    missing = expected_subset - set(df.columns)
    assert not missing, (
        f"pipeline_run_daily_summary is missing expected columns: {missing}"
    )

    # Basic type sanity for run_date
    if not df.empty:
        assert pd.api.types.is_datetime64_any_dtype(df["run_date"]) or \
               pd.api.types.is_object_dtype(df["run_date"]), (
            "Expected run_date to be a date-like column."
        )


# -------------------------------------------------------------------
# pipeline_ml_daily_summary
# -------------------------------------------------------------------

def test_pipeline_ml_daily_summary_exists(con):
    """
    Ensure the ML observability daily summary exists.
    """
    assert _table_or_view_exists(con, "pipeline_ml_daily_summary"), (
        "Expected table 'pipeline_ml_daily_summary' to exist. "
        "Run dbt observability models if this fails."
    )


def test_pipeline_ml_daily_summary_columns(con):
    """
    Check that pipeline_ml_daily_summary exposes core ML metrics per day/mode.
    """
    df = con.execute(
        'SELECT * FROM "pipeline_ml_daily_summary" LIMIT 1'
    ).df()

    expected_subset = {
        "run_date",
        "run_mode",
        "n_runs",
        "last_run_at",
        "last_n_train",
        "avg_accuracy",
        "avg_roc_auc",
    }
    missing = expected_subset - set(df.columns)
    assert not missing, (
        f"pipeline_ml_daily_summary is missing expected columns: {missing}"
    )

    # If there is data, do a bit more sanity checking
    if not df.empty:
        assert pd.api.types.is_numeric_dtype(df["n_runs"]), (
            "Expected n_runs to be numeric."
        )
        assert pd.api.types.is_numeric_dtype(df["avg_accuracy"]), (
            "Expected avg_accuracy to be numeric."
        )
        assert pd.api.types.is_numeric_dtype(df["avg_roc_auc"]), (
            "Expected avg_roc_auc to be numeric."
        )