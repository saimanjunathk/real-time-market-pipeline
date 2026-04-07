# Real-Time Market Data Pipeline + Anomaly Detection

End-to-end streaming pipeline for live market data with ML-based anomaly detection.

## Architecture
Alpaca/Binance WebSocket -> Kafka -> Spark Streaming -> ClickHouse -> Anomaly Detection -> Dashboard

## Tech Stack
- Ingestion: Python, WebSockets, Alpaca API
- Streaming: Apache Kafka, Apache Spark Streaming
- Storage: ClickHouse (columnar, time-series)
- ML: Scikit-learn (Isolation Forest), PyTorch (LSTM Autoencoder)
- Dashboard: Streamlit / Grafana
- Infra: Docker, Docker Compose

## Status
In Progress
