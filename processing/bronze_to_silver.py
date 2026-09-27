from pathlib import Path

from pyspark.sql import functions as F
from spark_session import get_spark

PROJECT_ROOT = Path(__file__).resolve().parents[1]

spark = get_spark("BronzeToSilver")

df = spark.read.format("delta").load(str(PROJECT_ROOT / "data/delta/bronze_bars"))

df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
df = df.dropna(subset=["open", "high", "low", "close", "volume", "timestamp"])
df = df.filter(F.col("volume") >= 0)

silver = df.dropDuplicates(["symbol","interval","timestamp"])

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
	str(PROJECT_ROOT / "data/delta/silver_bars")
)
print(f"Silver: {silver.count()} clean rows")

spark.stop()
