# Real-Time Market Data Pipeline + Anomaly Detection

## Live Demo
View Live Dashboard: https://real-time-market-pipeline.streamlit.app/

## Architecture
Market Data Simulator -> Kafka Queue (simulated) -> Spark Streaming (simulated) -> SQLite -> Isolation Forest Anomaly Detection -> Streamlit Dashboard

## Tech Stack
- Ingestion: Python, WebSocket simulation
- Message Queue: Python Queue (simulates Kafka)
- Stream Processing: Pandas (simulates Spark Streaming)
- Storage: SQLite (simulates ClickHouse)
- ML: Scikit-learn Isolation Forest
- Dashboard: Streamlit + Plotly

## Project Structure
- ingestion/alpaca_websocket.py - Simulates live market data feed
- ingestion/kafka_producer.py - Simulates Kafka message queue
- streaming/spark_consumer.py - Simulates Spark stream processing
- storage/clickhouse_writer.py - Writes to SQLite database
- anomaly_detection/isolation_forest.py - ML anomaly detection
- anomaly_detection/detector_pipeline.py - Full pipeline orchestration
- dashboard/app.py - Live Streamlit dashboard

## How to Run Locally
git clone https://github.com/saimanjunathk/real-time-market-pipeline
cd real-time-market-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run dashboard/app.py

## Status
- Market Data Simulator - Done
- Kafka Producer Simulation - Done
- Spark Stream Processing - Done
- SQLite Storage - Done
- Isolation Forest Anomaly Detection - Done
- Live Streamlit Dashboard - Done
- Deployed to Streamlit Cloud - Done