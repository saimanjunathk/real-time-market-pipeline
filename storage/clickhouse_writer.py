# This file simulates ClickHouse using SQLite
# ClickHouse is a columnar database optimized for time-series analytics
# SQLite is a lightweight file-based database
# Same SQL interface — easy to swap to real ClickHouse later
#
# Why ClickHouse in production?
# - Stores billions of rows efficiently
# - Queries like "average price per minute" run in milliseconds
# - Perfect for time-series market data

import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClickHouseWriter:

    def __init__(self, db_path: str = "data/market_data.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Create tables on startup
        self._init_tables()
        logger.info(f"Connected to database: {db_path}")


    def _init_tables(self):

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Main market data table
        # This stores every tick we receive
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                timestamp       TEXT NOT NULL,
                open            REAL,
                high            REAL,
                low             REAL,
                close           REAL,
                volume          INTEGER,
                price_change_pct REAL,
                rolling_mean    REAL,
                rolling_std     REAL,
                z_score         REAL,
                vwap            REAL,
                is_anomaly      INTEGER DEFAULT 0,
                anomaly_score   REAL DEFAULT 0,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Aggregated OHLCV per minute table
        # Pre-aggregated data for faster dashboard queries
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_ohlcv_1min (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT NOT NULL,
                minute      TEXT NOT NULL,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      INTEGER,
                tick_count  INTEGER,
                anomaly_count INTEGER DEFAULT 0,
                UNIQUE(symbol, minute)
            )
        """)

        conn.commit()
        conn.close()


    # This METHOD writes a DataFrame of ticks to the database
    def write(self, df: pd.DataFrame):

        if df.empty:
            return

        conn = sqlite3.connect(self.db_path)

        # Prepare columns that exist in both DataFrame and table
        columns = [
            "symbol", "timestamp", "open", "high", "low", "close",
            "volume", "price_change_pct", "rolling_mean", "rolling_std",
            "z_score", "vwap"
        ]

        # Add anomaly columns if they exist
        if "is_anomaly" in df.columns:
            columns += ["is_anomaly", "anomaly_score"]
        else:
            df["is_anomaly"]    = 0
            df["anomaly_score"] = 0.0
            columns += ["is_anomaly", "anomaly_score"]

        # Select only the columns we need
        insert_df = df[[c for c in columns if c in df.columns]].copy()

        # Convert timestamp to string for SQLite
        insert_df["timestamp"] = insert_df["timestamp"].astype(str)

        # to_sql writes DataFrame to SQLite table
        # if_exists="append" adds rows without dropping existing data
        insert_df.to_sql("market_ticks", conn, if_exists="append", index=False)

        # Update 1-minute OHLCV aggregation
        self._update_ohlcv(conn, insert_df)

        conn.commit()
        conn.close()

        logger.info(f"Wrote {len(df)} ticks to database")


    def _update_ohlcv(self, conn, df: pd.DataFrame):

        # Group ticks by symbol and minute
        df["minute"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M")

        for (symbol, minute), group in df.groupby(["symbol", "minute"]):
            conn.execute("""
                INSERT INTO market_ohlcv_1min
                    (symbol, minute, open, high, low, close, volume, tick_count, anomaly_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, minute) DO UPDATE SET
                    high          = MAX(high, excluded.high),
                    low           = MIN(low,  excluded.low),
                    close         = excluded.close,
                    volume        = volume + excluded.volume,
                    tick_count    = tick_count + excluded.tick_count,
                    anomaly_count = anomaly_count + excluded.anomaly_count
            """, (
                symbol, minute,
                float(group["open"].iloc[0]),
                float(group["high"].max()),
                float(group["low"].min()),
                float(group["close"].iloc[-1]),
                int(group["volume"].sum()),
                len(group),
                int(group["is_anomaly"].sum()) if "is_anomaly" in group else 0
            ))


    # This METHOD reads data from the database
    def read(self, query: str) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql(query, conn)
        conn.close()
        return df


    # This METHOD returns row count per table
    def stats(self) -> dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        ticks = cursor.execute("SELECT COUNT(*) FROM market_ticks").fetchone()[0]
        ohlcv = cursor.execute("SELECT COUNT(*) FROM market_ohlcv_1min").fetchone()[0]
        conn.close()
        return {"market_ticks": ticks, "market_ohlcv_1min": ohlcv}