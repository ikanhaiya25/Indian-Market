from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark

PROJECT_ROOT = Path(__file__).resolve().parents[1]

spark = get_spark("SilverToGold")

df = spark.read.format("delta").load(str(PROJECT_ROOT / "data/delta/silver_bars"))

w = Window.partitionBy("symbol", "interval").orderBy("timestamp")
session_window = (
	Window.partitionBy("symbol", "interval", F.to_date("timestamp"))
	.orderBy("timestamp")
	.rowsBetween(Window.unboundedPreceding, 0)
)

df = df.withColumn("sma_5", F.avg("close").over(w.rowsBetween(-4, 0)))
df = df.withColumn("sma_20", F.avg("close").over(w.rowsBetween(-19, 0)))

df = df.withColumn("prev_close", F.lag("close").over(w))
df = df.withColumn("delta", F.col("close") - F.col("prev_close"))
df = df.withColumn("gain", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0.0))
df = df.withColumn("loss", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0.0))

df = df.withColumn("avg_gain", F.avg("gain").over(w.rowsBetween(-13, 0)))
df = df.withColumn("avg_loss", F.avg("loss").over(w.rowsBetween(-13, 0)))
df = df.withColumn("rsi_14", F.when(
	F.col("avg_loss") == 0,
	F.when(F.col("avg_gain") == 0, 50.0).otherwise(100.0),
).otherwise(100.0 - (100.0 / (1.0 + F.col("avg_gain") / F.col("avg_loss")))))

typical_price = (F.col("high") + F.col("low") + F.col("close")) / 3.0
df = df.withColumn("cum_pv", F.sum(typical_price * F.col("volume")).over(session_window))
df = df.withColumn("cum_vol", F.sum("volume").over(session_window))
df = df.withColumn(
	"vwap",
	F.when(F.col("cum_vol") > 0, F.col("cum_pv") / F.col("cum_vol")),
)

gold = df.drop(
	"prev_close", "delta", "gain", "loss", "avg_gain", "avg_loss", "cum_pv", "cum_vol"
)

gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
	str(PROJECT_ROOT / "data/delta/gold_bars")
)

print(f"Gold: {gold.count()} rows with features")

spark.stop()