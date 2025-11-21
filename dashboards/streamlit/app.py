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

st.title("🌍 California vs Portugal — Climate Trends Dashboard")
st.caption("Data source: Open-Meteo | Model: DuckDB + dbt")

# -----------------------------------
# Database connection
# -----------------------------------

DB_PATH = "data/warehouse/climate.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def load_data():
    con = get_connection()

    query = """
    SELECT
        city_name,
        country_code,
        make_date(year, month, 1) as date,
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

    df = con.execute(query).df()
    return df


df = load_data()

if df.empty:
    st.error("❌ The climate table is empty. Make sure dbt has run successfully.")
    st.stop()

# -----------------------------------
# Sidebar controls
# -----------------------------------

st.sidebar.header("📊 Controls")

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
    index=0
)

selected_metric = metric_map[selected_metric_label]

cities = st.sidebar.multiselect(
    "Select cities",
    options=sorted(df["city_name"].unique()),
    default=sorted(df["city_name"].unique())
)

# Filter dataframe
df = df[df["city_name"].isin(cities)]

# -----------------------------------
# Main visualization
# -----------------------------------

fig = px.line(
    df,
    x="date",
    y=selected_metric,
    color="city_name",
    markers=True,
    title=f"{selected_metric_label} over time"
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title=selected_metric_label,
    template="plotly_white",
    legend_title="City",
    height=600
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------------
# Summary statistics
# -----------------------------------

st.subheader("📈 Summary statistics")

summary = (
    df
    .groupby("city_name")[selected_metric]
    .agg(["mean", "std", "min", "max"])
    .round(2)
    .reset_index()
)

st.dataframe(summary, use_container_width=True)

# -----------------------------------
# Raw data preview
# -----------------------------------

with st.expander("🔎 View underlying data"):
    st.dataframe(df.sort_values(["city_name", "date"]))