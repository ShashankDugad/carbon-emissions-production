from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat_ws
from pyspark.sql.types import DoubleType
import time

spark = SparkSession.builder.appName("EPA-Clean").getOrCreate()
start = time.time()

df = spark.read.parquet("hdfs:///user/hadoop/data/epa_raw")

# Combine date + time, then convert types
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
    col("`Sample Measurement`").cast(DoubleType()).alias("pm25"),
    col("Latitude").cast(DoubleType()).alias("latitude"),
    col("Longitude").cast(DoubleType()).alias("longitude")
).filter(
    col("timestamp_local").isNotNull() & 
    col("pm25").isNotNull() &
    (col("pm25") >= 0)
)

print(f"\n=== REPORT ===")
print(f"Input: {df.count():,}")
print(f"Output: {df_clean.count():,}")

df_clean.write.mode("overwrite").partitionBy("state_code").parquet("hdfs:///user/hadoop/data/epa_clean")

print(f"Time: {time.time()-start:.1f}s")
