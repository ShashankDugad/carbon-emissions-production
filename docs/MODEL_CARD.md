# Model Card: Air Quality Violation Predictor

## Model Details
- **Version:** 1.0
- **Date:** 2025-12-10
- **Type:** Binary classification
- **Framework:** PySpark MLlib

## Intended Use
Predict EPA air quality violations (PM2.5 > 35 μg/m³) 24-48 hours ahead for proactive alerts.

## Training Data
- **Size:** 34.3M hourly readings (2020-2023)
- **Features:** pm25_lag_24h, pm25_lag_48h, pm25_avg_7d, pm25_std_7d, hour, day_of_week, month
- **Label:** violation (1 if PM2.5 > 35, else 0)
- **Class balance:** 1.05% violations

## Evaluation
- **Test set:** 7.9M readings (2024)
- **Metrics:**
  - Random Forest: AUC 0.92, Acc 99.37%
  - Logistic Regression: AUC 0.88, Acc 99.34%

## Limitations
- Requires 7 days history for rolling features
- Performance degrades with missing lag data
- State name corruption in raw data (minor impact)

## Ethical Considerations
- Model may underperform in areas with sparse monitoring
- Should supplement, not replace, human decision-making
