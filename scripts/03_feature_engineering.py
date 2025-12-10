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

# Temporal features
df = df.withColumn("hour", hour("timestamp_local")) \
       .withColumn("day_of_week", dayofweek("timestamp_local")) \
       .withColumn("month", month("timestamp_local"))

# Window for lags (per site)
w = Window.partitionBy("state_code", "county_code", "site_num").orderBy("timestamp_local")

# Lag features
df = df.withColumn("pm25_lag_24h", lag("pm25", 24).over(w)) \
       .withColumn("pm25_lag_48h", lag("pm25", 48).over(w))

# Rolling windows (7 days = 168 hours)
w_roll = w.rowsBetween(-168, -1)
df = df.withColumn("pm25_avg_7d", avg("pm25").over(w_roll)) \
       .withColumn("pm25_std_7d", stddev("pm25").over(w_roll))

# Violation label
df = df.withColumn("violation", when(col("pm25") > 35, 1).otherwise(0))

# Drop nulls from lags
df = df.filter(col("pm25_lag_24h").isNotNull() & col("pm25_lag_48h").isNotNull())

print(f"Rows with features: {df.count():,}")
print(f"Violations: {df.filter(col('violation')==1).count():,}")

df.write.mode("overwrite").parquet("hdfs:///user/hadoop/data/features")
print(f"Time: {time.time()-start:.1f}s")
