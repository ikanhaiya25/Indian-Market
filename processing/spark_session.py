from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
  return(
    SparkSession.builder
    .appName(app_name)
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
  )