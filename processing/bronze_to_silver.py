from pyspark.sql import functions as F
from spark_session import get_spark

spark = get_spark("BronzeToSilver")

df = spark.read.format("delta").load("data/delta/bromze_bars")

df = df.withColumn("timestamps",F.to_timestamp("timestamp"))
df = df.dropna(subset=["open","high","low","close","volume"])
df = df.filter(F.col("volume")>= 0)

silver = df.dropDuplicates(["symbol","interval","timestamp"])

silver.write.format("delta").mode("overwrite").save("data/delta/silver_bars")
print(f"Silver: {silver.count()} clean rows")

spark.stop()
