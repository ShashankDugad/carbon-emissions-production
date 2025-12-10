"""
Feature Engineering for ML Models
Creates temporal features, lag features, rolling statistics, and violation labels.
Input: hdfs:///user/hadoop/data/epa_clean
Output: hdfs:///user/hadoop/data/features
"""
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev, hour, dayofweek, month, when
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("Features").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
start = time.time()

df = spark.read.parquet("hdfs:///user/hadoop/data/epa_clean")

# Extract temporal features
df = df.withColumn("hour", hour("timestamp_local")) \
       .withColumn("day_of_week", dayofweek("timestamp_local")) \
       .withColumn("month", month("timestamp_local"))

# Window partitioned by monitoring site, ordered by time
w = Window.partitionBy("state_code", "county_code", "site_num").orderBy("timestamp_local")

# Lag features for 24hr and 48hr predictions
df = df.withColumn("pm25_lag_24h", lag("pm25", 24).over(w)) \
       .withColumn("pm25_lag_48h", lag("pm25", 48).over(w))

# Rolling 7-day statistics (168 hours)
w_roll = w.rowsBetween(-168, -1)
df = df.withColumn("pm25_avg_7d", avg("pm25").over(w_roll)) \
       .withColumn("pm25_std_7d", stddev("pm25").over(w_roll))

# Violation label: PM2.5 > 35 μg/m³ (EPA standard)
df = df.withColumn("violation", when(col("pm25") > 35, 1).otherwise(0))

# Drop rows with null lags (first 48 hours per site)
df = df.filter(col("pm25_lag_24h").isNotNull() & col("pm25_lag_48h").isNotNull())

print(f"Rows with features: {df.count():,}")
print(f"Violations: {df.filter(col('violation')==1).count():,}")

df.write.mode("overwrite").parquet("hdfs:///user/hadoop/data/features")
print(f"Time: {time.time()-start:.1f}s")
