# Project Artifacts - Air Quality Violation Detection

## System Architecture
```
┌─────────────────┐
│   EPA Data      │
│   42M hourly    │
│   PM2.5 readings│
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│        AWS EMR Cluster              │
│  ┌─────────────────────────────┐   │
│  │  Data Ingestion             │   │
│  │  • CSV → Parquet            │   │
│  │  • Schema validation        │   │
│  │  • Type conversion          │   │
│  └───────────┬─────────────────┘   │
│              ▼                      │
│  ┌─────────────────────────────┐   │
│  │  Feature Engineering        │   │
│  │  • Lag features (24h, 48h)  │   │
│  │  • Rolling stats (7d)       │   │
│  │  • Temporal features        │   │
│  │  • Violation labels         │   │
│  └───────────┬─────────────────┘   │
│              ▼                      │
│  ┌─────────────────────────────┐   │
│  │  ML Training                │   │
│  │  • Random Forest (50 trees) │   │
│  │  • Logistic Regression      │   │
│  │  • Time-based split         │   │
│  └───────────┬─────────────────┘   │
│              ▼                      │
│  ┌─────────────────────────────┐   │
│  │  Kafka Streaming            │   │
│  │  • Real-time violations     │   │
│  │  • 3 partitions             │   │
│  └───────────┬─────────────────┘   │
└──────────────┼─────────────────────┘
               ▼
    ┌──────────────────────┐
    │  Streamlit Dashboard │
    │  • State filtering   │
    │  • Live predictions  │
    │  • Kafka alerts      │
    └──────────────────────┘
```

## Dataset Statistics

| Metric | Value |
|--------|-------|
| Total Records | 42,168,101 |
| Time Period | 2020-2024 (hourly) |
| Violations | 441,503 (1.05%) |
| States | 50+ |
| Storage (Raw) | 11 GB CSV |
| Storage (Processed) | 559 MB Parquet |

## Model Performance

### Random Forest (24-48hr Prediction)
- **AUC**: 0.9215
- **Accuracy**: 99.37%
- **Trees**: 50
- **Max Depth**: 10
- **Training Time**: 463s

### Logistic Regression (Real-time)
- **AUC**: 0.8804
- **Accuracy**: 99.34%
- **Iterations**: 10
- **Training Time**: 460s

## Processing Pipeline Timings

| Stage | Time | Input Rows | Output Rows |
|-------|------|------------|-------------|
| Ingestion | 334s | 43.1M | 43.1M |
| Cleaning | 76s | 43.1M | 42.2M |
| Features | 376s | 42.2M | 42.2M |
| Training | 923s | 34.3M train, 7.9M test | 2 models |

## Top Violation States

| Rank | State | Violation Rate |
|------|-------|----------------|
| 1 | Oregon | 3.82% |
| 2 | California | 3.15% |
| 3 | Montana | 2.47% |
| 4 | North Dakota | 2.16% |
| 5 | Washington | 1.70% |

## Infrastructure Details

### AWS EMR Cluster
- **ID**: j-NUMLZGH1220
- **Region**: ap-south-1 (Mumbai)
- **Release**: emr-7.12.0
- **Spark**: 3.5.6
- **Hadoop**: 3.4.1
- **Nodes**: 1 master + 2 core (m5.xlarge)
- **Resources**: 12 vCores, 48 GB RAM
- **Cost**: $0.58/hr (~$3.50 total pipeline)

### HDFS Storage
```
/user/hadoop/
├── data/
│   ├── epa_raw/           (70 MB)
│   ├── epa_clean/         (partitioned by state)
│   └── features/          (559 MB)
├── models/
│   ├── rf_model/          (9.7 KB)
│   └── lr_model/
└── outputs/
    └── county_stats/      (21.7 KB)
```

### Kafka Configuration
- **Topic**: air_quality
- **Partitions**: 3
- **Replication**: 1
- **Messages**: 100+ violations/sec

## Dashboard Features
- **URL**: http://<EMR-IP>:8501
- Real-time Kafka stream (10 alerts)
- State-level filtering
- Hourly/monthly violation patterns
- ML predictions (Current vs Wildfire scenarios)
- Interactive Plotly visualizations

## Code Quality Metrics
- **Scripts**: 8 Python files
- **Tests**: 1 data quality suite
- **Documentation**: README, MODEL_CARD, ARTIFACTS
- **Comments**: Comprehensive docstrings
- **Automation**: Makefile with 5 targets

## Key Technologies
- **Compute**: AWS EMR 7.12.0
- **Processing**: Apache Spark 3.5.6
- **Storage**: HDFS + Parquet (snappy compression)
- **Streaming**: Apache Kafka 3.9.0
- **ML**: Spark MLlib (Random Forest, Logistic Regression)
- **Visualization**: Streamlit + Plotly
- **Languages**: Python 3.9, PySpark

## Repository
- **URL**: https://github.com/ShashankDugad/carbon-emissions-production
- **Structure**: scripts/, tests/, docs/, reports/
- **CI/CD**: Ready for GitHub Actions
