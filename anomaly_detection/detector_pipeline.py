# This file wires everything together into one pipeline
# It starts all components and runs them concurrently using threads

import sys
import os
import time
import threading
import logging
import queue

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.alpaca_websocket import MarketDataSimulator
from ingestion.kafka_producer import KafkaProducerSimulator
from streaming.spark_consumer import StreamProcessor
from storage.clickhouse_writer import ClickHouseWriter
from anomaly_detection.isolation_forest import MarketAnomalyDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MarketDataPipeline:

    def __init__(self):

        # Step 1 — Create Kafka producer (message queue)
        self.producer = KafkaProducerSimulator(
            topic_name="market_data",
            max_size=5000
        )

        # Step 2 — Create market data simulator
        # It will send data directly into the producer's queue
        self.simulator = MarketDataSimulator(
            symbols=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "BTC", "ETH"],
            data_queue=self.producer.queue
        )

        # Step 3 — Create stream processor
        self.processor = StreamProcessor(
            producer=self.producer,
            batch_size=10,
            window_size=20
        )

        # Step 4 — Create database writer
        self.writer = ClickHouseWriter(
            db_path="data/market_data.db"
        )

        # Step 5 — Create anomaly detector
        self.detector = MarketAnomalyDetector(
            contamination=0.05,
            min_samples=50
        )

        logger.info("Pipeline initialized!")


    def start(self, duration_seconds: int = 60):

        logger.info(f"Starting pipeline for {duration_seconds} seconds...")

        # Start simulator in background thread
        self.simulator.start(interval_seconds=0.5)

        # Start stream processor in background thread
        processor_thread = threading.Thread(
            target=self.processor.run,
            kwargs={
                "writer":   self.writer,
                "detector": self.detector
            },
            daemon=True
        )
        processor_thread.start()

        # Run for specified duration
        try:
            time.sleep(duration_seconds)
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted by user")

        # Stop everything
        self.simulator.stop()
        logger.info(f"Pipeline complete! Stats: {self.writer.stats()}")


if __name__ == "__main__":
    pipeline = MarketDataPipeline()
    pipeline.start(duration_seconds=30)