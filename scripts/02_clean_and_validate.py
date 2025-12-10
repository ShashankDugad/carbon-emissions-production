"""
Data Cleaning & Validation
Type conversions, null filtering, and data quality checks.
Input: hdfs:///user/hadoop/data/epa_raw
Output: hdfs:///user/hadoop/data/epa_clean (partitioned by state)
"""
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat_ws
from pyspark.sql.types import DoubleType
import time

spark = SparkSession.builder.appName("EPA-Clean").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
start = time.time()

df = spark.read.parquet("hdfs:///user/hadoop/data/epa_raw")

# Combine date and time columns, then convert to timestamp
df_clean = df.withColumn(
    "timestamp_local",
    to_timestamp(concat_ws(" ", col("`Date Local`"), col("`Time Local`")), "yyyy-MM-dd HH:mm")
).select(
    col("`State Code`").alias("state_code"),
    col("`County Code`").alias("county_code"),
    col("`Site Num`").alias("site_num"),
    col("`State Name`").alias("state_name"),
    col("`County Name`").alias("county_name"),
    col("timestamp_local"),
    col("`Sample Measurement`").cast(DoubleType()).alias("pm25"),  # Convert to numeric
    col("Latitude").cast(DoubleType()).alias("latitude"),
    col("Longitude").cast(DoubleType()).alias("longitude")
).filter(
    # Remove invalid records
    col("timestamp_local").isNotNull() & 
    col("pm25").isNotNull() &
    (col("pm25") >= 0)  # PM2.5 cannot be negative
)

print(f"\n=== REPORT ===")
print(f"Input: {df.count():,}")
print(f"Output: {df_clean.count():,}")

# Partition by state for efficient filtering
df_clean.write.mode("overwrite").partitionBy("state_code").parquet("hdfs:///user/hadoop/data/epa_clean")

print(f"Time: {time.time()-start:.1f}s")
