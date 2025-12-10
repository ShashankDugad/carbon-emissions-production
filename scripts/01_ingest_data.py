from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder.appName("EPA-Ingest").getOrCreate()
start = time.time()

df = spark.read.csv("hdfs:///user/hadoop/data/hourly_88101_*.csv", header=True, inferSchema=True)

print(f"\n=== PROFILE ===")
print(f"Rows: {df.count():,}")
print(f"Cols: {len(df.columns)}")
df.printSchema()

print(f"\n=== NULLS (first 10) ===")
for c in df.columns[:10]:
    null_ct = df.filter(col(c).isNull()).count()
    print(f"{c}: {null_ct:,}")

df.write.mode("overwrite").parquet("hdfs:///user/hadoop/data/epa_raw")

print(f"\nTime: {time.time()-start:.1f}s")
