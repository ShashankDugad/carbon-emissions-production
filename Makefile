.PHONY: setup data clean train test report all

all: data train test report

setup:
	pip3 install numpy

data:
	spark-submit --master yarn scripts/01_ingest_data.py
	spark-submit --master yarn scripts/02_clean_and_validate.py
	spark-submit --master yarn scripts/03_feature_engineering.py

train:
	spark-submit --master yarn scripts/04_train_models.py

test:
	spark-submit tests/test_data_quality.py

report:
	spark-submit --master yarn scripts/05_batch_analytics.py

clean:
	hdfs dfs -rm -r -f /user/hadoop/data/*
	hdfs dfs -rm -r -f /user/hadoop/models/*
