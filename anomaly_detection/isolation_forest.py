# Isolation Forest is an ML algorithm for anomaly detection
# It works by randomly isolating points in the data
# Normal points need many splits to isolate (they blend in)
# Anomalies need very few splits to isolate (they stand out)
#
# Perfect for market data because:
# - No need for labeled training data
# - Works well with high-dimensional features
# - Fast and scalable
# - Handles multivariate anomalies (price + volume + volatility together)

import numpy as np
import pandas as pd
import logging
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MarketAnomalyDetector:

    # contamination → expected proportion of anomalies in data
    #                 0.05 means we expect 5% of ticks to be anomalies
    # min_samples   → minimum ticks needed before we start detecting
    #                 need enough data to learn what "normal" looks like
    def __init__(self, contamination: float = 0.05, min_samples: int = 50):
        self.contamination = contamination
        self.min_samples   = min_samples

        # IsolationForest from scikit-learn
        # n_estimators=100 → build 100 isolation trees (more = more accurate)
        # random_state=42  → reproducible results
        self.model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42
        )

        # StandardScaler normalizes features to same scale
        # Without scaling, volume (10000s) dominates price (100s)
        self.scaler = StandardScaler()

        # Store all ticks seen so far for training
        self.history = []
        self.is_trained = False

        logger.info(f"Anomaly detector initialized | contamination={contamination}")


    # These are the FEATURES we use for anomaly detection
    # More features = better detection but slower training
    FEATURES = [
        "close",            # Current price
        "price_change_pct", # Price change this tick
        "volume",           # Trading volume
        "z_score",          # How far from rolling mean
        "rolling_std",      # Recent volatility
    ]


    # This METHOD trains the model on historical data
    def train(self, df: pd.DataFrame):

        # Only train if we have all required features
        available = [f for f in self.FEATURES if f in df.columns]
        if len(available) < 3:
            return

        # Add to history
        self.history.append(df)
        all_data = pd.concat(self.history, ignore_index=True)

        # Only train if we have enough samples
        if len(all_data) < self.min_samples:
            logger.info(f"Need {self.min_samples - len(all_data)} more samples to train")
            return

        # Extract feature matrix X
        X = all_data[available].fillna(0).values
        # fillna(0) replaces any NaN with 0 to avoid training errors

        # Scale features so they're all on similar scale
        X_scaled = self.scaler.fit_transform(X)
        # fit_transform learns the scale AND applies it in one step

        # Train the Isolation Forest
        self.model.fit(X_scaled)
        self.is_trained = True

        logger.info(f"Model trained on {len(all_data)} samples")


    # This METHOD detects anomalies in a batch of ticks
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()

        # Add default anomaly columns
        df["is_anomaly"]    = 0
        df["anomaly_score"] = 0.0

        # Train on this batch first
        self.train(df)

        # Can only detect if model is trained
        if not self.is_trained:
            return df

        available = [f for f in self.FEATURES if f in df.columns]
        X = df[available].fillna(0).values
        X_scaled = self.scaler.transform(X)
        # transform applies the SAME scaling learned during training
        # Never call fit_transform on new data — that would change the scale!

        # predict() returns 1 for normal, -1 for anomaly
        predictions = self.model.predict(X_scaled)

        # decision_function() returns anomaly score
        # More negative = more anomalous
        scores = self.model.decision_function(X_scaled)

        # Convert to our format: 1=anomaly, 0=normal
        df["is_anomaly"]    = (predictions == -1).astype(int)
        df["anomaly_score"] = scores

        anomaly_count = df["is_anomaly"].sum()
        if anomaly_count > 0:
            logger.warning(f"Detected {anomaly_count} anomalies in batch!")

        return df


# Test it directly
if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Generate fake data to test
    np.random.seed(42)
    n = 100

    df = pd.DataFrame({
        "close":            np.random.normal(175, 2, n),
        "price_change_pct": np.random.normal(0, 0.1, n),
        "volume":           np.random.randint(1000, 50000, n),
        "z_score":          np.random.normal(0, 1, n),
        "rolling_std":      np.random.uniform(0.5, 2.0, n),
    })

    # Inject obvious anomalies
    df.loc[50, "price_change_pct"] = 15.0   # 15% spike
    df.loc[75, "price_change_pct"] = -12.0  # 12% crash

    detector = MarketAnomalyDetector(min_samples=50)
    result = detector.detect(df)

    print(f"Total ticks: {len(result)}")
    print(f"Anomalies detected: {result['is_anomaly'].sum()}")
    print("\nTop anomalies:")
    print(result[result["is_anomaly"] == 1][
        ["close", "price_change_pct", "anomaly_score"]
    ].head())