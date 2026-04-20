# This file simulates a live market data WebSocket feed
# In production this connects to Alpaca or Binance WebSocket API
# We simulate it by generating realistic OHLCV data every second
# OHLCV = Open, High, Low, Close, Volume — standard market data format

import random
import time
import queue
import threading
from datetime import datetime
from faker import Faker

fake = Faker()
random.seed(None)  # None means truly random each run


class MarketDataSimulator:

    # symbols → list of stock/crypto symbols to simulate
    # data_queue → shared queue where we put generated data
    #              other parts of the pipeline read from this queue
    def __init__(self, symbols: list, data_queue: queue.Queue):
        self.symbols = symbols
        self.data_queue = data_queue

        # Starting prices for each symbol
        # We'll simulate price movements from these base prices
        self.prices = {
            "AAPL":  175.0,
            "GOOGL": 140.0,
            "MSFT":  380.0,
            "AMZN":  185.0,
            "TSLA":  250.0,
            "BTC":   45000.0,
            "ETH":   2500.0,
        }

        # is_running controls whether the simulator keeps running
        # Setting it to False stops the simulation
        self.is_running = False


    # This METHOD generates one realistic market data tick
    # A tick = one price update for one symbol
    def generate_tick(self, symbol: str) -> dict:

        # Get current price for this symbol
        current_price = self.prices[symbol]

        # Simulate price movement using random walk
        # random.gauss(mean, std) generates a normally distributed number
        # Mean=0 means price is equally likely to go up or down
        # Std=0.001 means 0.1% typical move per tick (realistic for 1-second data)
        price_change = random.gauss(0, 0.001)

        # Occasionally inject a large price spike (anomaly)
        # 2% chance of anomaly on each tick
        if random.random() < 0.02:
            # Large spike: 2-5% move — this is what our anomaly detector will catch
            price_change = random.gauss(0, 0.03)

        # Calculate new price
        new_price = current_price * (1 + price_change)

        # Update stored price for next tick
        self.prices[symbol] = new_price

        # Generate OHLCV data for this tick
        # In real markets, open=previous close, high/low fluctuate around close
        open_price  = current_price
        close_price = new_price
        high_price  = max(open_price, close_price) * (1 + abs(random.gauss(0, 0.0005)))
        low_price   = min(open_price, close_price) * (1 - abs(random.gauss(0, 0.0005)))
        volume      = random.randint(1000, 50000)

        return {
            "symbol":     symbol,
            "timestamp":  datetime.now().isoformat(),
            "open":       round(open_price,  4),
            "high":       round(high_price,  4),
            "low":        round(low_price,   4),
            "close":      round(close_price, 4),
            "volume":     volume,
            # Price change as percentage
            "price_change_pct": round(price_change * 100, 4)
        }


    # This METHOD runs continuously in a background thread
    # It generates ticks and puts them in the queue
    def stream(self, interval_seconds: float = 1.0):

        self.is_running = True
        print(f"Starting market data stream for: {self.symbols}")

        while self.is_running:
            for symbol in self.symbols:
                tick = self.generate_tick(symbol)

                # put() adds the tick to the queue
                # Other threads can then get() it from the queue
                # This is how we simulate Kafka producer → consumer
                self.data_queue.put(tick)

            # Wait before generating next batch of ticks
            time.sleep(interval_seconds)


    # This METHOD starts the stream in a background thread
    # daemon=True means thread stops when main program stops
    def start(self, interval_seconds: float = 1.0):
        thread = threading.Thread(
            target=self.stream,
            args=(interval_seconds,),
            daemon=True
        )
        thread.start()
        return thread


    def stop(self):
        self.is_running = False
        print("Market data stream stopped")


# Test it directly
if __name__ == "__main__":
    # Create a shared queue
    data_queue = queue.Queue()

    # Create simulator with 3 symbols
    simulator = MarketDataSimulator(
        symbols=["AAPL", "GOOGL", "MSFT"],
        data_queue=data_queue
    )

    # Start streaming
    simulator.start(interval_seconds=1.0)

    # Read and print 10 ticks
    print("Reading market data ticks:")
    for i in range(10):
        tick = data_queue.get()  # get() blocks until data is available
        print(f"  {tick['symbol']} | Close: {tick['close']} | Change: {tick['price_change_pct']}%")

    simulator.stop()