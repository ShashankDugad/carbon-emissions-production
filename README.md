# Carbon Emissions Violation Detection

Production ML system for predicting EPA air quality violations 24-48 hours in advance.

## Quick Start
```bash
make setup    # Install dependencies
make data     # Download & process EPA data
make train    # Train models
make test     # Run validation tests
make report   # Generate results
```

## Architecture
- Data: 42M hourly PM2.5 readings (2020-2024)
- Features: Lag (24h, 48h), rolling stats (7d), temporal
- Models: Logistic Regression (realtime), Random Forest (prediction)
- Storage: HDFS Parquet, partitioned by state

## Performance
| Model | AUC | Accuracy | Use Case |
|-------|-----|----------|----------|
| LR    | 0.88 | 99.34%  | Realtime classification |
| RF    | 0.92 | 99.37%  | 24-48hr prediction |

## Setup
- AWS EMR 7.12.0 (Spark 3.5.6, Hadoop 3.4.1)
- 3 nodes: m5.xlarge (12 vCores, 48GB RAM)
- Cost: ~$3.50 for full pipeline

## Files
- `scripts/01_ingest_data.py` - Load raw CSVs to Parquet
- `scripts/02_clean_and_validate.py` - Type conversion, filtering
- `scripts/03_feature_engineering.py` - Lag features, labels
- `scripts/04_train_models.py` - Train LR & RF models
- `scripts/05_batch_analytics.py` - State/county aggregations
- `tests/test_data_quality.py` - Data validation suite
