.PHONY: data train test stream clean

data:
	spark-submit --master yarn scripts/01_ingest_data.py
	spark-submit --master yarn scripts/02_clean_and_validate.py
	spark-submit --master yarn scripts/03_feature_engineering.py

train:
	spark-submit --master yarn scripts/04_train_models.py

test:
	spark-submit tests/test_data_quality.py

stream:
	streamlit run scripts/08_streamlit_dashboard.py --server.port 8501 --server.address 0.0.0.0

clean:
	hdfs dfs -rm -r -f /user/hadoop/data/* /user/hadoop/models/*
