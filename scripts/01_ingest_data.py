"""
Data Ingestion Pipeline
Loads EPA PM2.5 hourly data from CSV files and converts to Parquet format.
Input: CSV files in /user/hadoop/data/
Output: Parquet in hdfs:///user/hadoop/data/epa_raw
"""
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Initialize Spark session
spark = SparkSession.builder.appName("EPA-Ingest").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
start = time.time()

# Load all CSV files with automatic schema inference
df = spark.read.csv("hdfs:///user/hadoop/data/hourly_88101_*.csv", header=True, inferSchema=True)

print(f"\n=== DATA PROFILE ===")
print(f"Rows: {df.count():,}")
print(f"Cols: {len(df.columns)}")
df.printSchema()

# NULL analysis for first 10 columns
print(f"\n=== NULLS (first 10) ===")
for c in df.columns[:10]:
    null_ct = df.filter(col(c).isNull()).count()
    print(f"{c}: {null_ct:,}")

# Save as compressed Parquet for efficient storage and querying
df.write.mode("overwrite").parquet("hdfs:///user/hadoop/data/epa_raw")

print(f"\nTime: {time.time()-start:.1f}s")
