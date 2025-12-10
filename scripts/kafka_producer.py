"""Kafka Producer: Stream violation events from recent data"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from kafka import KafkaProducer
import json
import time

spark = SparkSession.builder.appName("Producer").getOrCreate()
df = spark.read.parquet("hdfs:///user/hadoop/data/features") \
    .filter(col("violation")==1).limit(100)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for row in df.collect():
    message = {
        'state': row['state_name'],
        'county': row['county_name'],
        'pm25': float(row['pm25']),
        'timestamp': str(row['timestamp_local'])
    }
    producer.send('air_quality', value=message)
    print(f"Sent: {message['state']} - PM2.5: {message['pm25']}")
    time.sleep(0.5)

producer.flush()
print("Done: 100 violations sent")
