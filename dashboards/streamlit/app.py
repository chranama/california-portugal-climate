import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from climate_pipeline.utils.get_paths import get_duckdb_path

# -----------------------------------
# App config
# -----------------------------------

st.set_page_config(
    page_title="California vs Portugal Climate",
    layout="wide"
)

st.title("🌍 California vs Portugal — Climate & Pipeline Observability")
st.caption("Data source: Open-Meteo | Stack: DuckDB + dbt + Streamlit + ML")


# -----------------------------------
# Database connection
# -----------------------------------

DB_PATH = str(get_duckdb_path())


@st.cache_resource
def get_connection():
    # read_only keeps things safe from accidental writes
    return duckdb.connect(DB_PATH, read_only=True)


# -----------------------------------
# Data loaders
# -----------------------------------

@st.cache_data
def load_climate_data() -> pd.DataFrame:
    con = get_connection()
    query = """
        SELECT
            city_name,
            country_code,
            make_date(year, month, 1) AS date,
            avg_tmax_c,
            avg_tmin_c,
            avg_tmean_c,
            avg_dewpoint_c,
            avg_wind_max_ms,
            avg_sw_radiation,
            total_precip_mm,
            heat_day_count,
            tropical_night_count,
            heavy_precip_day_count,
            summer_day_count
        FROM clean_monthly_climate
        ORDER BY date
    """
    return con.execute(query).df()

# -----------------------------------
# Helper to load arbitrary DuckDB tables
# -----------------------------------

@st.cache_data
def load_table(table_name: str):
    """Load any DuckDB table into a pandas DataFrame."""
    con = get_connection()
    try:
        return con.execute(f"SELECT * FROM {table_name}").df()
    except Exception as e:
        st.warning(f"Could not load table '{table_name}': {e}")
        return pd.DataFrame()


# -----------------------------------
# Observability loaders
# -----------------------------------

def load_pipeline_run_summary() -> pd.DataFrame:
    """Load daily pipeline run summary from DuckDB for the UI."""
    con = get_connection()
    try:
        return con.execute(
            """
            SELECT
                run_date,
                flow_name,
                run_mode,

                -- Names expected by Streamlit UI
                runs_success AS success_count,
                runs_failed  AS failure_count,
                runs_total   AS n_runs,

                -- Convenience metric used in the UI
                CASE
                    WHEN runs_total > 0
                        THEN runs_success * 1.0 / runs_total
                    ELSE NULL
                END AS success_rate,

                -- Volume / delta columns
                rows_bronze_max,
                rows_gold_ml_max,
                rows_bronze_delta_max,
                rows_gold_ml_delta_max
            FROM pipeline_run_daily_summary
            ORDER BY run_date DESC, run_mode
            """
        ).df()
    except Exception as e:
        st.warning(f"Could not load pipeline_run_daily_summary: {e}")
        return pd.DataFrame()


def load_pipeline_ml_summary() -> pd.DataFrame:
    """Load daily ML metrics summary from DuckDB."""
    con = get_connection()
    try:
        return con.execute(
            """
            SELECT
                run_date,
                run_mode,
                n_runs,
                last_run_at,
                last_n_train,
                avg_accuracy,
                avg_roc_auc
            FROM pipeline_ml_daily_summary
            ORDER BY run_date DESC, run_mode
            """
        ).df()
    except Exception as e:
        st.warning(f"Could not load pipeline_ml_daily_summary: {e}")
        return pd.DataFrame()


# -----------------------------------
# Load main climate data
# -----------------------------------

climate_df = load_climate_data()

if climate_df.empty:
    st.error("❌ The climate table is empty. Make sure dbt has run successfully.")
    st.stop()


# -----------------------------------
# Sidebar controls (for climate view)
# -----------------------------------

st.sidebar.header("📊 Climate controls")

metric_map = {
    "Average Mean Temperature (°C)": "avg_tmean_c",
    "Average Max Temperature (°C)": "avg_tmax_c",
    "Average Min Temperature (°C)": "avg_tmin_c",
    "Total Precipitation (mm)": "total_precip_mm",
    "Average Max Wind Speed (m/s)": "avg_wind_max_ms",
    "Average Dew Point (°C)": "avg_dewpoint_c",
    "Shortwave Radiation": "avg_sw_radiation",
    "Heat Day Count": "heat_day_count",
    "Tropical Night Count": "tropical_night_count",
    "Heavy Precipitation Days": "heavy_precip_day_count",
    "Summer Day Count": "summer_day_count",
}

selected_metric_label = st.sidebar.selectbox(
    "Select a climate metric",
    list(metric_map.keys()),
    index=0,
)

selected_metric = metric_map[selected_metric_label]

cities = st.sidebar.multiselect(
    "Select cities",
    options=sorted(climate_df["city_name"].unique()),
    default=sorted(climate_df["city_name"].unique()),
)

# Filter climate dataframe
climate_filtered = climate_df[climate_df["city_name"].isin(cities)]


# -----------------------------------
# Main layout: Tabs
# -----------------------------------

climate_tab, observability_tab = st.tabs(
    ["🌡️ Climate trends", "🧪 Pipeline & ML observability"]
)


# -----------------------------------
# Tab 1: Climate trends
# -----------------------------------

with climate_tab:
    st.subheader("🌡️ Monthly climate trends")

    fig = px.line(
        climate_filtered,
        x="date",
        y=selected_metric,
        color="city_name",
        markers=True,
        title=f"{selected_metric_label} over time",
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=selected_metric_label,
        template="plotly_white",
        legend_title="City",
        height=600,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📈 Summary statistics")

    summary = (
        climate_filtered
        .groupby("city_name")[selected_metric]
        .agg(["mean", "std", "min", "max"])
        .round(2)
        .reset_index()
    )

    st.dataframe(summary, use_container_width=True)

    with st.expander("🔎 View underlying climate data"):
        st.dataframe(
            climate_filtered.sort_values(["city_name", "date"]),
            use_container_width=True,
        )


# -----------------------------------
# Tab 2: Pipeline & ML observability
# -----------------------------------

with observability_tab:
    st.subheader("🧪 Pipeline run & ML model health")

    run_df = load_pipeline_run_summary()
    ml_df = load_pipeline_ml_summary()

    if run_df.empty and ml_df.empty:
        st.info(
            "No observability data found yet. "
            "Run the Prefect daily flow and ML training to populate the tables."
        )
    else:
        col_runs, col_ml = st.columns(2)

        # ----------------------------
        # Left: pipeline run summary
        # ----------------------------
        with col_runs:
            st.markdown("### 🧱 Pipeline runs (daily)")

            if run_df.empty:
                st.warning("No rows in `pipeline_run_daily_summary` yet.")
            else:
                # Basic derived metrics
                run_df = run_df.copy()
                run_df["total_runs"] = (
                    run_df["success_count"] + run_df["failure_count"]
                )
                run_df["success_rate"] = (
                    run_df["success_count"] / run_df["total_runs"].replace(0, pd.NA)
                )

                # Success rate chart
                fig_sr = px.line(
                    run_df,
                    x="run_date",
                    y="success_rate",
                    color="run_mode",
                    markers=True,
                    title="Success rate by day & run_mode",
                )
                fig_sr.update_layout(
                    xaxis_title="Run date",
                    yaxis_title="Success rate",
                    template="plotly_white",
                    legend_title="Run mode",
                    height=350,
                )
                st.plotly_chart(fig_sr, use_container_width=True)

                # Volume chart
                fig_vol = px.bar(
                    run_df,
                    x="run_date",
                    y="total_runs",
                    color="run_mode",
                    title="Number of runs per day",
                )
                fig_vol.update_layout(
                    xaxis_title="Run date",
                    yaxis_title="Total runs",
                    template="plotly_white",
                    legend_title="Run mode",
                    height=300,
                )
                st.plotly_chart(fig_vol, use_container_width=True)

                with st.expander("📋 Daily pipeline summary table"):
                    st.dataframe(
                        run_df.sort_values(["run_date", "run_mode", "flow_name"]),
                        use_container_width=True,
                    )

        # ----------------------------
        # Right: ML metrics summary
        # ----------------------------
        with col_ml:
            st.subheader("😃 ML metrics (daily)")

            ml_df = load_table("pipeline_ml_daily_summary")

            if ml_df.empty:
                st.info(
                    "No ML metrics found yet. Run the ML training step "
                    "(e.g. `uv run climate-train-baseline`) to populate the table."
                )
            else:
                # Make sure we have the new minority-class columns
                expected_cols = {
                    "run_date",
                    "run_mode",
                    "avg_accuracy",
                    "avg_roc_auc",
                    "avg_precision_pos",
                    "avg_recall_pos",
                    "avg_f1_pos",
                }
                missing = expected_cols - set(ml_df.columns)
                if missing:
                    st.warning(
                        f"ML summary table is missing expected columns: {sorted(missing)}. "
                        "Did you run `dbt build --select observability` after updating the models?"
                    )
                else:
                    # Top-level summary for the most recent day
                    latest = (
                        ml_df.sort_values("run_date")
                        .groupby("run_mode")
                        .tail(1)
                        .sort_values("run_date", ascending=False)
                    )

                    st.markdown("#### Latest daily ML metrics (per run mode)")
                    metric_cols = st.columns(min(3, len(latest)))

                    for col, (_, row) in zip(metric_cols, latest.iterrows()):
                        with col:
                            st.metric(
                                label=f"{row['run_mode']} – ROC-AUC",
                                value=f"{row['avg_roc_auc']:.3f}",
                            )
                            st.metric(
                                label=f"{row['run_mode']} – F1 (event=1)",
                                value=f"{row['avg_f1_pos']:.3f}",
                            )

                    # Time-series plots
                    col_left, col_right = st.columns(2)

                    with col_left:
                        st.markdown("##### Average ROC-AUC by day & run_mode")
                        fig_roc = px.line(
                            ml_df,
                            x="run_date",
                            y="avg_roc_auc",
                            color="run_mode",
                            markers=True,
                            labels={
                                "run_date": "Run date",
                                "avg_roc_auc": "Average ROC-AUC",
                                "run_mode": "Run mode",
                            },
                        )
                        fig_roc.update_layout(height=350)
                        st.plotly_chart(fig_roc, use_container_width=True)

                    with col_right:
                        st.markdown("##### Average accuracy by day & run_mode")
                        fig_acc = px.line(
                            ml_df,
                            x="run_date",
                            y="avg_accuracy",
                            color="run_mode",
                            markers=True,
                            labels={
                                "run_date": "Run date",
                                "avg_accuracy": "Average accuracy",
                                "run_mode": "Run mode",
                            },
                        )
                        fig_acc.update_layout(height=350)
                        st.plotly_chart(fig_acc, use_container_width=True)

                    st.markdown("##### Minority-class F1 (event=1) by day & run_mode")
                    fig_f1 = px.line(
                        ml_df,
                        x="run_date",
                        y="avg_f1_pos",
                        color="run_mode",
                        markers=True,
                        labels={
                            "run_date": "Run date",
                            "avg_f1_pos": "Average F1 (class 1 / event)",
                            "run_mode": "Run mode",
                        },
                    )
                    fig_f1.update_layout(height=350)
                    st.plotly_chart(fig_f1, use_container_width=True)