import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# -----------------------------------
# App config
# -----------------------------------

st.set_page_config(
    page_title="California vs Portugal Climate",
    layout="wide"
)

st.title("🌍 California vs Portugal — Climate & Pipeline Observability")
st.caption("Data source: Open-Meteo | Warehouse: DuckDB + dbt | Orchestration: Prefect")

# -----------------------------------
# Database connection
# -----------------------------------

DB_PATH = "data/warehouse/climate.duckdb"


@st.cache_resource
def get_connection():
    # read_only because dashboard should never mutate the DB
    return duckdb.connect(DB_PATH, read_only=True)


# -------- Climate data loaders --------

@st.cache_data
def load_climate_monthly():
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
        FROM silver_monthly_climate
        ORDER BY date
    """
    return con.execute(query).df()


# -------- Observability loaders --------

@st.cache_data
def load_pipeline_runs(limit: int = 200):
    con = get_connection()
    query = f"""
        SELECT
            id,
            flow_name,
            run_mode,
            status,
            started_at,
            finished_at,
            rows_bronze,
            rows_gold_ml,
            rows_bronze_delta,
            rows_gold_ml_delta,
            bronze_max_date,
            gold_ml_max_date,
            freshness_status
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT {limit}
    """
    return con.execute(query).df()


@st.cache_data
def load_pipeline_daily_summary():
    con = get_connection()
    query = """
        SELECT
            run_date,
            flow_name,
            run_mode,
            runs_total,
            runs_success,
            runs_failed,
            rows_bronze_max,
            rows_gold_ml_max,
            rows_bronze_delta_max,
            rows_gold_ml_delta_max
        FROM pipeline_run_daily_summary
        ORDER BY run_date
    """
    return con.execute(query).df()


# -----------------------------------
# Tabs: Climate vs Observability
# -----------------------------------

tab_climate, tab_obs = st.tabs(
    ["🌤 Climate Trends", "🛰 Pipeline Observability"]
)

# ===================================
# 🌤 Climate Trends tab
# ===================================
with tab_climate:
    df_climate = load_climate_monthly()

    if df_climate.empty:
        st.error("❌ The climate table is empty. Make sure dbt has run successfully.")
        st.stop()

    st.subheader("📊 Climate Trends")

    st.sidebar.header("📊 Controls (Climate)")

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
        "Select a metric",
        list(metric_map.keys()),
        index=0,
        key="climate_metric_select",
    )

    selected_metric = metric_map[selected_metric_label]

    cities = st.sidebar.multiselect(
        "Select cities",
        options=sorted(df_climate["city_name"].unique()),
        default=sorted(df_climate["city_name"].unique()),
        key="climate_city_multiselect",
    )

    df_filtered = df_climate[df_climate["city_name"].isin(cities)]

    fig = px.line(
        df_filtered,
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
        df_filtered
        .groupby("city_name")[selected_metric]
        .agg(["mean", "std", "min", "max"])
        .round(2)
        .reset_index()
    )

    st.dataframe(summary, use_container_width=True)

    with st.expander("🔎 View underlying climate data"):
        st.dataframe(df_filtered.sort_values(["city_name", "date"]))

# ===================================
# 🛰 Pipeline Observability tab
# ===================================
with tab_obs:
    st.subheader("🛰 Pipeline Health & Run History")

    df_runs = load_pipeline_runs(limit=200)
    df_daily = load_pipeline_daily_summary()

    if df_runs.empty:
        st.info(
            "No pipeline runs logged yet. "
            "Trigger `uv run climate-prefect-daily` to populate pipeline_run_log."
        )
    else:
        # --- Topline metrics ---
        col1, col2, col3, col4 = st.columns(4)

        last_run = df_runs.iloc[0]
        last_status = last_run["status"]
        last_rows_bronze = int(last_run["rows_bronze"])
        last_rows_gold_ml = int(last_run["rows_gold_ml"])
        freshness = last_run.get("freshness_status", "unknown")

        with col1:
            st.metric("Last run status", last_status)
        with col2:
            st.metric("Bronze rows (last run)", f"{last_rows_bronze:,}")
        with col3:
            st.metric("Gold ML rows (last run)", f"{last_rows_gold_ml:,}")
        with col4:
            st.metric("Data freshness", freshness)

        st.markdown("---")

        # --- Daily run chart ---
        if not df_daily.empty:
            st.markdown("### 📆 Daily run outcomes")

            fig_runs = px.bar(
                df_daily,
                x="run_date",
                y=["runs_success", "runs_failed"],
                title="Daily pipeline run counts",
                labels={"value": "Runs", "run_date": "Date", "variable": "Status"},
            )
            fig_runs.update_layout(
                barmode="stack",
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_runs, use_container_width=True)

        # --- Row growth over time (from daily summary) ---
        if not df_daily.empty:
            st.markdown("### 📦 Warehouse row growth (max per day)")

            fig_rows = px.line(
                df_daily,
                x="run_date",
                y=["rows_bronze_max", "rows_gold_ml_max"],
                title="Rows in bronze / gold_ml over time (daily max)",
                labels={"value": "Row count", "run_date": "Date", "variable": "Table"},
            )
            fig_rows.update_layout(
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_rows, use_container_width=True)

        st.markdown("### 🧾 Recent runs")

        # A compact table for the last N runs
        cols = [
            "id",
            "started_at",
            "finished_at",
            "flow_name",
            "run_mode",
            "status",
            "rows_bronze",
            "rows_gold_ml",
            "rows_bronze_delta",
            "rows_gold_ml_delta",
            "bronze_max_date",
            "gold_ml_max_date",
            "freshness_status",
        ]
        st.dataframe(df_runs[cols], use_container_width=True, hide_index=True)