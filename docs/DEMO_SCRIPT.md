# 15-Minute Presentation Demo Script

## Slide 1: Title (30 sec)
**Say:** "Air Quality Violation Detection using AWS EMR and Spark MLlib"
**Do:** Introduce team, semester

## Slide 2: Problem Statement (1 min)
**Say:** "EPA monitors 442K violations annually. Current systems are reactive. We built predictive ML to detect violations 24-48 hours ahead."
**Show:** Violation definition (PM2.5 > 35 μg/m³)

## Slide 3: Solution Overview (1 min)
**Say:** "End-to-end pipeline on AWS EMR processing 42M hourly records with Spark, training ML models, and streaming real-time alerts via Kafka."
**Highlight:** AWS EMR, not GCP

## Slide 4: Dataset (1 min)
**Say:** "EPA Air Quality System: 42 million hourly PM2.5 readings from 2020-2024 across 50 states."
**Show:** Table from ARTIFACTS.md

## Slide 5: Tech Stack (30 sec)
**Say:** "AWS EMR 7.12, Spark 3.5.6, HDFS storage, Kafka streaming, MLlib models."
**Point:** Production-grade infrastructure

## Slide 6: Architecture (2 min)
**Say:** "Data flows from EPA CSVs → EMR ingestion → feature engineering → ML training → Kafka streaming → Streamlit dashboard."
**Show:** Architecture diagram from ARTIFACTS.md
**Walk through:** Each component

## Slide 7: Live Dashboard Demo (4 min)
**Do:** Open http://ec2-13-203-232-73.ap-south-1.compute.amazonaws.com:8501

**Demo Flow:**
1. **Metrics** (10 sec): "42M records, 442K violations, RF AUC 0.92"
2. **Model Performance** (15 sec): "Random Forest outperforms LR for 24-48hr prediction"
3. **Top States** (20 sec): "Oregon leads at 3.82% - wildfire season impact"
4. **State Filter** (30 sec): Select California, show hourly/monthly patterns
5. **ML Prediction** (45 sec):
   - Select Oregon
   - Choose "Wildfire Season"
   - Click "Predict 24hrs"
   - Show: "23/24 hours with violations"
6. **Kafka Stream** (90 sec):
   - SSH to EMR in second terminal: `python3 scripts/kafka_producer.py`
   - Click "Start Stream" in dashboard
   - **Show live alerts appearing in sidebar**
   - Explain: "Real-time violation detection streaming through Kafka"

## Slide 8: Model Results (1 min)
**Say:** "Random Forest: 92.15% AUC, trained on 34M samples with time-based split to prevent leakage."
**Show:** Training time, accuracy metrics

## Slide 9: Key Findings (1 min)
**Say:** "Oregon has highest violations due to wildfires. Clear seasonal patterns in August-September. Hourly patterns show afternoon peaks."
**Show:** State rankings, monthly trends

## Slide 10: Challenges (1 min)
**Say:** "Key challenges: AWS security groups for ports, PySpark path configuration in Streamlit, handling 11GB CSV efficiently."
**Explain:** How we solved each

## Slide 11: Future Work (30 sec)
**Say:** "Weather data integration, deployment automation, model retraining pipeline, multi-pollutant detection."

## Slide 12: Q&A (Remaining time)

---

## Backup Demos (If Time)

### Show Code Quality
```bash
cat scripts/03_feature_engineering.py  # Show docstrings
```

### Show HDFS Storage
```bash
hdfs dfs -ls -h /user/hadoop/data/
hdfs dfs -ls /user/hadoop/models/
```

### Show Kafka Topic
```bash
~/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Run Batch Analytics
```bash
spark-submit scripts/05_batch_analytics.py
```

---

## Common Questions & Answers

**Q: Why 42M vs 286M rows?**
A: Switched from daily to hourly data for better temporal resolution in predictions.

**Q: Why time-based split?**
A: Prevents data leakage - model can't see future during training.

**Q: How does 24-48hr prediction work?**
A: Uses lag features (previous 24h, 48h values) and rolling 7-day statistics.

**Q: Cost?**
A: $3.50 for complete pipeline run. $0.58/hr for EMR cluster.

**Q: Why Random Forest over other models?**
A: Handles non-linear patterns well, resistant to outliers, interpretable feature importance.

**Q: Real-world deployment?**
A: Add model monitoring, auto-retraining, alert system, API endpoints.

---

## Technical Backup

### If Dashboard Fails
Show static HTML: `reports/dashboard.html`

### If Kafka Fails
Explain: "Kafka streams violations in real-time. Producer sends events, Spark Streaming processes them."

### If Questions on Scale
- 42M rows processed in 14 minutes
- Parquet compression: 11GB → 559MB (95% reduction)
- Model training: <15 minutes on 3-node cluster
