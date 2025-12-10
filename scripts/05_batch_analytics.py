import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, month

spark = SparkSession.builder.appName("Analytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs:///user/hadoop/data/features")

# Top 10 violation states
print("=== TOP 10 VIOLATION STATES ===")
df.groupBy("state_name").agg(
    count(col("violation")).alias("total_readings"),
    avg(col("violation")).alias("violation_rate")
).orderBy(col("violation_rate").desc()).limit(10).show()

# Monthly trend
print("=== MONTHLY VIOLATION TREND ===")
df.withColumn("year", year("timestamp_local")) \
  .withColumn("month_num", month("timestamp_local")) \
  .groupBy("year", "month_num").agg(
      count("*").alias("readings"),
      avg("violation").alias("violation_rate")
  ).orderBy("year", "month_num").show(24)

# Save outputs
df.groupBy("state_name", "county_name").agg(
    count(col("violation")).alias("total"),
    avg(col("pm25")).alias("avg_pm25")
).write.mode("overwrite").parquet("hdfs:///user/hadoop/outputs/county_stats")

print("Saved to hdfs:///user/hadoop/outputs/")
