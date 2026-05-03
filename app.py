"""
Real-Time Anomaly Detection Dashboard
Streamlit app that reads from DuckDB and visualises pipeline output.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.getenv("PIPELINE_DB", "data/pipeline.duckdb")

st.set_page_config(
    page_title="E-Commerce Anomaly Monitor",
    page_icon="🔍",
    layout="wide",
)

# ─── Load Data ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=10)
def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT * FROM orders").df()
    con.close()
    return df


def run_pipeline_and_reload():
    """Trigger producer + ETL and reload data."""
    from producer.event_producer import produce_to_file
    from etl.pipeline import run_pipeline
    produce_to_file("data/raw_events.jsonl", n_events=100)
    run_pipeline("data/raw_events.jsonl", DB_PATH)
    st.cache_data.clear()


# ─── UI ───────────────────────────────────────────────────────────────────────

st.title("🔍 Real-Time E-Commerce Anomaly Detection")
st.caption("Kafka-backed streaming pipeline · DuckDB · GitHub Actions CI/CD")

col_run, col_refresh = st.columns([2, 8])
with col_run:
    if st.button("▶ Run Pipeline", type="primary", use_container_width=True):
        with st.spinner("Running ETL pipeline..."):
            run_pipeline_and_reload()
        st.success("Pipeline complete!")
        st.rerun()

df = load_data()

if df.empty:
    st.warning("No data yet. Click **Run Pipeline** to generate and process events.")
    st.stop()

# ─── KPI Cards ────────────────────────────────────────────────────────────────

anomalies = df[df["is_anomaly"] == True]
total = len(df)
n_anomalies = len(anomalies)
anomaly_rate = n_anomalies / total * 100 if total else 0
avg_severity = anomalies["severity_score"].mean() if n_anomalies else 0
total_value_at_risk = anomalies["total_value"].sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Events", f"{total:,}")
k2.metric("Anomalies Detected", f"{n_anomalies:,}", f"{anomaly_rate:.1f}% of traffic")
k3.metric("Avg Severity Score", f"{avg_severity:.1f} / 100")
k4.metric("Value at Risk", f"${total_value_at_risk:,.2f}")

st.divider()

# ─── Charts ───────────────────────────────────────────────────────────────────

left, right = st.columns(2)

with left:
    st.subheader("Anomaly Types Breakdown")
    if n_anomalies > 0:
        flag_series = (
            anomalies["anomaly_flags"]
            .str.split(",")
            .explode()
            .value_counts()
            .reset_index()
        )
        flag_series.columns = ["Anomaly Type", "Count"]
        fig = px.pie(
            flag_series, values="Count", names="Anomaly Type",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No anomalies detected.")

with right:
    st.subheader("Revenue by Category (Normal vs Anomalous)")
    cat_df = df.groupby(["category", "is_anomaly"])["total_value"].sum().reset_index()
    cat_df["type"] = cat_df["is_anomaly"].map({True: "Anomalous", False: "Normal"})
    fig2 = px.bar(
        cat_df, x="category", y="total_value", color="type",
        barmode="group",
        color_discrete_map={"Normal": "#2ecc71", "Anomalous": "#e74c3c"},
        labels={"total_value": "Total Value ($)", "category": "Category"},
    )
    st.plotly_chart(fig2, use_container_width=True)

# ─── Severity Distribution ────────────────────────────────────────────────────

st.subheader("Severity Score Distribution (Anomalous Events)")
if n_anomalies > 0:
    fig3 = px.histogram(
        anomalies, x="severity_score", nbins=20,
        color_discrete_sequence=["#e74c3c"],
        labels={"severity_score": "Severity Score (0–100)"},
    )
    fig3.update_layout(bargap=0.1)
    st.plotly_chart(fig3, use_container_width=True)

# ─── Anomaly Table ────────────────────────────────────────────────────────────

st.subheader("⚠️ Anomaly Event Log")
display_cols = [
    "event_id", "timestamp", "user_id", "product_name", "category",
    "quantity", "unit_price", "base_price", "price_ratio",
    "session_clicks", "anomaly_flags", "severity_score", "total_value"
]
st.dataframe(
    anomalies[display_cols].sort_values("severity_score", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.caption(f"Data source: `{DB_PATH}` · {total:,} total events processed")
