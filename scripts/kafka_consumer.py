"""Spark Streaming Consumer: Process violation events"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("Consumer").getOrCreate()

schema = StructType() \
    .add("state", StringType()) \
    .add("county", StringType()) \
    .add("pm25", DoubleType()) \
    .add("timestamp", StringType())

stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality") \
    .load()

violations = stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = violations.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
