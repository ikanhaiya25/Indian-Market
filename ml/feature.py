# Destination: ml/feature.py
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_PATH = PROJECT_ROOT / "data" / "delta" / "gold_bars"
DATASET_PATH = PROJECT_ROOT / "ml" / "dataset.parquet"

df = DeltaTable(str(GOLD_PATH)).to_pandas()
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.sort_values(["symbol", "interval", "timestamp"])

keys = ["symbol", "interval"]
grouped = df.groupby(keys, sort=False)
df["next_close"] = grouped["close"].shift(-1)
df["next_timestamp"] = grouped["timestamp"].shift(-1)
df = df.dropna(subset=["next_close", "next_timestamp", "sma_5", "sma_20", "rsi_14", "vwap"])
df["target"] = (df["next_close"] > df["close"]).astype("int8")

features = ["sma_5", "sma_20", "rsi_14", "vwap", "volume"]
DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
df[features + ["target", "symbol", "interval", "timestamp", "next_timestamp"]].to_parquet(
    DATASET_PATH,
    index=False,
)
print(f"Wrote {len(df)} labeled rows to {DATASET_PATH}")
