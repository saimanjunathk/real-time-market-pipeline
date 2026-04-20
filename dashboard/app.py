import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import threading
import queue
import time
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.alpaca_websocket import MarketDataSimulator
from ingestion.kafka_producer import KafkaProducerSimulator
from streaming.spark_consumer import StreamProcessor
from storage.clickhouse_writer import ClickHouseWriter
from anomaly_detection.isolation_forest import MarketAnomalyDetector

# ── Page config ──
st.set_page_config(
    page_title="Real-Time Market Pipeline",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Market Data Pipeline")
st.markdown("**Simulated Kafka → Spark Streaming → SQLite → Anomaly Detection**")
st.divider()

DB_PATH = "data/market_data.db"


# ── Initialize pipeline in session state ──
# st.session_state persists values across page reruns
# Without this, pipeline would restart every time user clicks anything
if "pipeline_started" not in st.session_state:
    st.session_state.pipeline_started = False

if "writer" not in st.session_state:
    st.session_state.writer = ClickHouseWriter(db_path=DB_PATH)

if "producer" not in st.session_state:
    st.session_state.producer = KafkaProducerSimulator("market_data", max_size=5000)

if "simulator" not in st.session_state:
    st.session_state.simulator = MarketDataSimulator(
        symbols=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
        data_queue=st.session_state.producer.queue
    )

if "processor" not in st.session_state:
    st.session_state.processor = StreamProcessor(
        producer=st.session_state.producer,
        batch_size=5,
        window_size=20
    )

if "detector" not in st.session_state:
    st.session_state.detector = MarketAnomalyDetector(
        contamination=0.05,
        min_samples=30
    )


# ── Sidebar controls ──
with st.sidebar:
    st.header("⚙️ Pipeline Controls")

    if not st.session_state.pipeline_started:
        if st.button("▶️ Start Pipeline", type="primary", use_container_width=True):
            # Start simulator
            st.session_state.simulator.start(interval_seconds=0.3)

            # Start processor in background
            t = threading.Thread(
                target=st.session_state.processor.run,
                kwargs={
                    "writer":     st.session_state.writer,
                    "detector":   st.session_state.detector,
                    "max_batches": 10000
                },
                daemon=True
            )
            t.start()
            st.session_state.pipeline_started = True
            st.rerun()
    else:
        st.success("✅ Pipeline Running")
        if st.button("⏹️ Stop Pipeline", use_container_width=True):
            st.session_state.simulator.stop()
            st.session_state.pipeline_started = False
            st.rerun()

    st.divider()
    refresh = st.slider("Refresh every (seconds)", 2, 10, 3)
    st.divider()

    st.markdown("""
    **Architecture:**
    - 🎯 Alpaca WebSocket (simulated)
    - 📨 Kafka Producer (simulated)
    - ⚡ Spark Streaming (simulated)
    - 🗄️ ClickHouse (SQLite)
    - 🤖 Isolation Forest ML
    - 📊 Streamlit Dashboard

    **[View on GitHub](https://github.com/saimanjunathk/real-time-market-pipeline)**
    """)


# ── Main dashboard ──
writer = st.session_state.writer

if not st.session_state.pipeline_started:
    st.info("👈 Click **Start Pipeline** in the sidebar to begin streaming market data!")
    st.stop()

# Read latest data
try:
    df = writer.read("""
        SELECT * FROM market_ticks
        ORDER BY timestamp DESC
        LIMIT 500
    """)
except Exception:
    st.warning("⏳ Waiting for data...")
    time.sleep(2)
    st.rerun()

if df.empty:
    st.warning("⏳ Waiting for first batch...")
    time.sleep(2)
    st.rerun()

df["timestamp"] = pd.to_datetime(df["timestamp"])


# ── KPIs ──
st.subheader("📊 Live Statistics")
col1, col2, col3, col4 = st.columns(4)

stats = writer.stats()
anomalies = df["is_anomaly"].sum() if "is_anomaly" in df.columns else 0
symbols   = df["symbol"].nunique()

with col1:
    st.metric("📨 Total Ticks", f"{stats['market_ticks']:,}")
with col2:
    st.metric("📈 Symbols", symbols)
with col3:
    st.metric("🚨 Anomalies", int(anomalies))
with col4:
    latest_price = df[df["symbol"] == "AAPL"]["close"].iloc[0] if "AAPL" in df["symbol"].values else 0
    st.metric("🍎 AAPL Price", f"${latest_price:.2f}")

st.divider()


# ── Price Charts ──
st.subheader("💹 Live Price Feed")

selected = st.selectbox("Select Symbol", df["symbol"].unique())
sym_df = df[df["symbol"] == selected].sort_values("timestamp")

fig = go.Figure()

# Price line
fig.add_trace(go.Scatter(
    x=sym_df["timestamp"],
    y=sym_df["close"],
    mode="lines",
    name="Price",
    line=dict(color="#00d4ff", width=2)
))

# Rolling mean line
if "rolling_mean" in sym_df.columns:
    fig.add_trace(go.Scatter(
        x=sym_df["timestamp"],
        y=sym_df["rolling_mean"],
        mode="lines",
        name="Rolling Mean",
        line=dict(color="#ffa500", width=1, dash="dot")
    ))

# Anomaly markers
if "is_anomaly" in sym_df.columns:
    anomaly_df = sym_df[sym_df["is_anomaly"] == 1]
    if not anomaly_df.empty:
        fig.add_trace(go.Scatter(
            x=anomaly_df["timestamp"],
            y=anomaly_df["close"],
            mode="markers",
            name="Anomaly",
            marker=dict(color="red", size=10, symbol="x")
        ))

fig.update_layout(
    title=f"{selected} Price Stream",
    xaxis_title="Time",
    yaxis_title="Price ($)",
    height=400
)
st.plotly_chart(fig, use_container_width=True)

st.divider()


# ── Anomaly Detection ──
st.subheader("🚨 Anomaly Detection")
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Z-Score Distribution**")
    if "z_score" in df.columns:
        fig = px.histogram(
            df, x="z_score", color="symbol",
            nbins=50,
            labels={"z_score": "Z-Score"}
        )
        fig.add_vline(x=2,  line_dash="dash", line_color="red",  annotation_text="+2σ")
        fig.add_vline(x=-2, line_dash="dash", line_color="red",  annotation_text="-2σ")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Recent Anomalies**")
    if "is_anomaly" in df.columns:
        anomaly_df = df[df["is_anomaly"] == 1][[
            "symbol", "timestamp", "close",
            "price_change_pct", "z_score"
        ]].head(10)
        if anomaly_df.empty:
            st.info("No anomalies detected yet — keep the pipeline running!")
        else:
            st.dataframe(anomaly_df, hide_index=True, use_container_width=True)

st.divider()


# ── Volume Chart ──
st.subheader("📊 Volume by Symbol")
vol_df = df.groupby("symbol")["volume"].sum().reset_index()
fig = px.bar(vol_df, x="symbol", y="volume", color="symbol",
             labels={"volume": "Total Volume", "symbol": "Symbol"})
st.plotly_chart(fig, use_container_width=True)


# ── Auto refresh ──
time.sleep(refresh)
st.rerun()