"""
ML Model Training
Trains Logistic Regression (realtime) and Random Forest (24-48hr prediction).
Input: hdfs:///user/hadoop/data/features
Output: Models in hdfs:///user/hadoop/models/
"""
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import time

spark = SparkSession.builder.appName("Train").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
start = time.time()

df = spark.read.parquet("hdfs:///user/hadoop/data/features")

# Time-based split prevents data leakage
train = df.filter(col("timestamp_local") < "2024-01-01")
test = df.filter(col("timestamp_local") >= "2024-01-01")

print(f"Train: {train.count():,} | Test: {test.count():,}")

# Assemble feature vector
feature_cols = ["pm25_lag_24h", "pm25_lag_48h", "pm25_avg_7d", "pm25_std_7d", "hour", "day_of_week", "month"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

train_vec = assembler.transform(train).select("features", "violation")
test_vec = assembler.transform(test).select("features", "violation")

# Model 1: Logistic Regression (fast, realtime classification)
print("\n=== LOGISTIC REGRESSION ===")
lr = LogisticRegression(featuresCol="features", labelCol="violation", maxIter=10)
lr_model = lr.fit(train_vec)
lr_pred = lr_model.transform(test_vec)

auc_eval = BinaryClassificationEvaluator(labelCol="violation", metricName="areaUnderROC")
multi_eval = MulticlassClassificationEvaluator(labelCol="violation", predictionCol="prediction")

print(f"AUC: {auc_eval.evaluate(lr_pred):.4f}")
print(f"Accuracy: {multi_eval.evaluate(lr_pred, {multi_eval.metricName: 'accuracy'}):.4f}")

lr_model.write().overwrite().save("hdfs:///user/hadoop/models/lr_model")

# Model 2: Random Forest (higher accuracy, 24-48hr prediction)
print("\n=== RANDOM FOREST ===")
rf = RandomForestClassifier(featuresCol="features", labelCol="violation", numTrees=50, maxDepth=10)
rf_model = rf.fit(train_vec)
rf_pred = rf_model.transform(test_vec)

print(f"AUC: {auc_eval.evaluate(rf_pred):.4f}")
print(f"Accuracy: {multi_eval.evaluate(rf_pred, {multi_eval.metricName: 'accuracy'}):.4f}")

rf_model.write().overwrite().save("hdfs:///user/hadoop/models/rf_model")

print(f"\nTotal: {time.time()-start:.1f}s")
