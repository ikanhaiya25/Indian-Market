from pyspark.sql import functions as F
from pyspark.sql import Window
from spark_session import get_spark

spark = get_spark("SilverToGold")

df = spark.read.format("delta").load("data/delta/silver_bars")

w = Window.partitionBy("symbol").orderBy("timestamp")
w_unbounded = w.rowsBetween(Window.unboundedPreceding, 0)

df = df.withColumn("sma_5", F.avg("close").over(w.rowsBetween(-4,0)))

df = df.withColumn("sma_20", F.avg("close").over(w.rowsBetween))

df = df.withColumn("prev_close",F.lag("close") - F.col("prev_close"))

df = df.withColumn("gain", F.when(F.col("delta") > 0,F.col("delta")).otherwise(0.0))

df = df.withColumn("loss", F.when(F.col("delta") < 0, - F.col("delta")).otherwise(0.0))

df = df.withColumn("avg_gain", F.avg("gain").over(w.rowsBetween(-13,0)))

df = df.withColumn("avg_loss", F.avg("loss").over(w.rowsBetween(-13,0)))

df = df.withColumn("rs",F.col("avg_gain") / F.col("avg_loss"))

df = df.withColumn("rsi_14", F.lit(100) - (F.lit(100) / (F.lit(1) + F.col("rs"))))

