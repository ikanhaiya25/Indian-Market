import sys 
from pathlib import Path
import pandas as pd 
import clickhouse_connect

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "processing"))

from spark_session import get_spark

PROJECT_ROOT = Path(__file__).resolve().parents[1]

spark = get_spark("LoadGoldToClickHouse")

sdf = spark.read.format("delta").load(str(PROJECT_ROOT / "data/delta/gold_bars"))
columns = [
    "symbol", "interval", "timestamp", "open", "high", "low", "close",
    "volume", "sma_5", "sma_20", "rsi_14", "vwap",
]
df = pd.DataFrame.from_records(
  (row.asDict(recursive=True) for row in sdf.select(*columns).toLocalIterator()),
    columns=columns,
)

for column in ("sma_5", "sma_20", "rsi_14", "vwap"):
  df[column] = df[column].astype(object).where(pd.notnull(df[column]), None)

try:
  client = clickhouse_connect.get_client(host="localhost", port=8123)
  client.insert_df("market_bars", df)
  print(f"Loaded {len(df)} rows into ClickHouse")
finally:
  spark.stop()
