# This file simulates a Kafka producer using Python's queue module
# In production: KafkaProducer sends messages to Kafka topics
# Our version:   queue.Queue acts as the message bus between threads
#
# Kafka concept recap:
# Producer  → puts messages into a topic (queue)
# Topic     → named channel for messages (our queue)
# Consumer  → reads messages from the topic
# This pattern decouples data generation from data processing

import queue
import threading
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaProducerSimulator:

    # topic_name  → name of the "topic" (just for logging, like real Kafka)
    # max_size    → maximum messages in queue before blocking
    #               prevents memory overflow if consumer is slow
    def __init__(self, topic_name: str, max_size: int = 1000):
        self.topic_name = topic_name

        # queue.Queue is thread-safe
        # Multiple threads can put/get without data corruption
        self.queue = queue.Queue(maxsize=max_size)

        self.message_count = 0
        logger.info(f"Producer initialized for topic: {topic_name}")


    # This METHOD sends a message to the queue
    # message → any Python dict (our market data tick)
    def send(self, message: dict):

        # Add metadata to every message
        # This is what real Kafka does — adds offset, timestamp, partition
        enriched_message = {
            "topic":      self.topic_name,
            "offset":     self.message_count,  # message sequence number
            "timestamp":  datetime.now().isoformat(),
            "payload":    message              # actual data
        }

        # put_nowait() adds to queue without blocking
        # Raises queue.Full if queue is full
        try:
            self.queue.put_nowait(enriched_message)
            self.message_count += 1

            # Log every 50 messages to avoid too much noise
            if self.message_count % 50 == 0:
                logger.info(f"Topic '{self.topic_name}': {self.message_count} messages sent")

        except queue.Full:
            logger.warning(f"Queue full! Dropping message for {message.get('symbol')}")


    # This METHOD reads the next message from the queue
    # timeout → how long to wait for a message before giving up
    def consume(self, timeout: float = 1.0) -> dict:
        try:
            # get() blocks until a message is available or timeout
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            return None


    # This METHOD returns current queue size
    def queue_size(self) -> int:
        return self.queue.qsize()


    # This METHOD returns stats about the producer
    def stats(self) -> dict:
        return {
            "topic":         self.topic_name,
            "total_sent":    self.message_count,
            "queue_size":    self.queue_size(),
        }


# Test it directly
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ingestion.alpaca_websocket import MarketDataSimulator

    # Create producer
    producer = KafkaProducerSimulator(topic_name="market_data")

    # Create simulator that sends to producer
    simulator = MarketDataSimulator(
        symbols=["AAPL", "TSLA", "BTC"],
        data_queue=producer.queue
    )

    # Wrap simulator to use producer.send()
    simulator.start(interval_seconds=0.5)

    # Consume 15 messages
    print("\nConsuming messages from topic 'market_data':")
    for i in range(15):
        msg = producer.consume(timeout=2.0)
        if msg:
            payload = msg["payload"]
            print(f"  Offset {msg['offset']:3d} | {payload['symbol']:5s} | ${payload['close']:.2f}")

    print(f"\nStats: {producer.stats()}")
    simulator.stop()