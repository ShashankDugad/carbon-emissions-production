# Air Quality Violation Detection System

Production ML pipeline for predicting EPA air quality violations 24-48 hours ahead using AWS EMR, Spark MLlib, and Kafka streaming.

## Quick Start
```bash
# Setup cluster
aws emr create-cluster --name "Air-Quality-Prod" --release-label emr-7.5.0 \
  --applications Name=Spark Name=Hadoop --instance-type m5.xlarge --instance-count 3

# Run pipeline
make data    # Ingest 42M EPA records
make train   # Train RF (AUC 0.92) + LR (AUC 0.88)
make stream  # Start Kafka dashboard
```

## Architecture
- **Data**: 42M hourly PM2.5 readings (2020-2024)
- **Storage**: HDFS Parquet, partitioned by state
- **Features**: Lag (24h, 48h), rolling stats (7d), temporal
- **Models**: Random Forest (prediction), Logistic Regression (realtime)
- **Streaming**: Kafka + Spark Structured Streaming
- **Dashboard**: Streamlit with live violation alerts

## Performance
| Model | AUC | Accuracy | Use Case |
|-------|-----|----------|----------|
| RF | 0.9215 | 99.37% | 24-48hr prediction |
| LR | 0.8804 | 99.34% | Real-time classification |

## Repository Structure
```
├── scripts/          # ETL and ML pipeline
├── tests/            # Data quality tests
├── docs/             # MODEL_CARD.md
├── reports/          # Analytics outputs
└── Makefile          # Automation
```

## Demo
Dashboard: `http://<EMR-IP>:8501`
- Interactive state filtering
- Live Kafka violation stream
- Real-time ML predictions

## Setup
```bash
pip3 install streamlit plotly kafka-python
export PYTHONPATH=/usr/lib/spark/python:$PYTHONPATH
streamlit run scripts/08_streamlit_dashboard.py --server.port 8501
```

## Data Source
EPA Air Quality System: https://aqs.epa.gov/aqsweb/airdata/download_files.html

## License
MIT
