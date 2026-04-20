# This file simulates Apache Spark Streaming using Pandas
# In production: Spark reads from Kafka, processes in micro-batches
# Our version:   We read from our queue in batches and process with Pandas
#
# Spark Streaming concept:
# Micro-batch → collect N messages → process as a batch → repeat
# This gives near-real-time processing without true record-by-record overhead

import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StreamProcessor:

    # producer      → our KafkaProducerSimulator to read from
    # batch_size    → how many messages to process at once (micro-batch)
    # window_size   → how many recent ticks to keep per symbol for calculations
    def __init__(self, producer, batch_size: int = 10, window_size: int = 20):
        self.producer    = producer
        self.batch_size  = batch_size
        self.window_size = window_size

        # deque is a double-ended queue with max length
        # When it's full and you append, it automatically removes the oldest item
        # Perfect for sliding windows of recent data
        # We keep a separate window per symbol
        self.windows = {}

        self.processed_count = 0
        logger.info(f"Stream processor ready | batch={batch_size} | window={window_size}")


    # This METHOD collects a batch of messages from the queue
    def collect_batch(self) -> list:
        batch = []
        for _ in range(self.batch_size):
            msg = self.producer.consume(timeout=0.5)
            if msg is None:
                continue
            # Handle both raw ticks and Kafka-wrapped messages
            if isinstance(msg, dict) and "payload" in msg:
                 batch.append(msg["payload"])
            else:
                batch.append(msg)
        return batch


    # This METHOD processes a batch of ticks
    # Returns a DataFrame with enriched features
    def process_batch(self, batch: list) -> pd.DataFrame:

        if not batch:
            return pd.DataFrame()

        # Convert list of dicts to DataFrame
        df = pd.DataFrame(batch)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Process each symbol separately
        enriched_rows = []

        for symbol in df["symbol"].unique():

            # Get rows for this symbol only
            symbol_df = df[df["symbol"] == symbol].copy()

            # Initialize window for this symbol if first time seeing it
            if symbol not in self.windows:
                self.windows[symbol] = deque(maxlen=self.window_size)

            # Add new prices to the window
            for price in symbol_df["close"].values:
                self.windows[symbol].append(price)

            window = list(self.windows[symbol])

            # Calculate technical features if we have enough data
            if len(window) >= 5:

                window_arr = np.array(window)

                # Rolling mean = average of last N prices
                # Used to detect if current price deviates from average
                rolling_mean = np.mean(window_arr)

                # Rolling std = standard deviation of last N prices
                # Measures how volatile the price has been
                rolling_std  = np.std(window_arr)

                # Z-score = how many standard deviations from mean
                # Z-score > 2 or < -2 usually indicates an anomaly
                # Formula: (current - mean) / std
                for _, row in symbol_df.iterrows():
                    z_score = (row["close"] - rolling_mean) / (rolling_std + 1e-8)
                    # 1e-8 prevents division by zero

                    # VWAP = Volume Weighted Average Price
                    # More accurate price measure than simple average
                    # Formula: sum(price * volume) / sum(volume)
                    vwap = np.average(
                        window_arr[-len(symbol_df):],
                        weights=symbol_df["volume"].values[:len(window_arr[-len(symbol_df):])]
                    ) if len(symbol_df) > 0 else row["close"]

                    enriched_rows.append({
                        **row.to_dict(),
                        "rolling_mean": round(rolling_mean, 4),
                        "rolling_std":  round(rolling_std,  4),
                        "z_score":      round(z_score,      4),
                        "vwap":         round(float(vwap),  4),
                        "window_size":  len(window),
                    })
            else:
                # Not enough data yet — add rows without features
                for _, row in symbol_df.iterrows():
                    enriched_rows.append({
                        **row.to_dict(),
                        "rolling_mean": row["close"],
                        "rolling_std":  0.0,
                        "z_score":      0.0,
                        "vwap":         row["close"],
                        "window_size":  len(window),
                    })

        self.processed_count += len(enriched_rows)
        return pd.DataFrame(enriched_rows)


    # This METHOD runs continuously processing batches
    def run(self, writer=None, detector=None, max_batches: int = None):

        logger.info("Stream processor started")
        batch_num = 0

        while True:
            # Collect a batch of messages
            batch = self.collect_batch()

            if batch:
                # Process the batch
                df = self.process_batch(batch)

                if not df.empty:
                    # Run anomaly detection if detector provided
                    if detector:
                        df = detector.detect(df)

                    # Write to storage if writer provided
                    if writer:
                        writer.write(df)

                    batch_num += 1
                    logger.info(
                        f"Batch {batch_num}: processed {len(df)} ticks | "
                        f"total={self.processed_count}"
                    )

            # Stop if max_batches reached (useful for testing)
            if max_batches and batch_num >= max_batches:
                logger.info(f"Reached max batches ({max_batches}), stopping")
                break

            time.sleep(0.1)