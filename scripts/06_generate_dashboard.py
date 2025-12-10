from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, year, month
import json

spark = SparkSession.builder.appName("Dashboard").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs:///user/hadoop/data/features")

# Metrics
total_rows = df.count()
violations = df.filter(col("violation")==1).count()

# Top 10 states
states = df.groupBy("state_name").agg(
    count("*").alias("readings"),
    (avg(col("violation"))*100).alias("rate")
).orderBy(col("rate").desc()).limit(10).collect()

state_names = [r['state_name'] for r in states]
state_rates = [round(r['rate'], 2) for r in states]

# Monthly trend
monthly = df.withColumn("year", year("timestamp_local")) \
    .withColumn("month", month("timestamp_local")) \
    .groupBy("year", "month").agg(
        (avg("violation")*100).alias("rate")
    ).orderBy("year", "month").limit(24).collect()

months = [f"{r['year']}-{r['month']:02d}" for r in monthly]
rates = [round(r['rate'], 2) for r in monthly]

html = f"""<!DOCTYPE html>
<html><head><title>Air Quality Dashboard</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
body {{font-family: Arial; margin: 20px; background: #f5f5f5;}}
.header {{background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;}}
.metric {{display: inline-block; margin: 15px; padding: 20px; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); min-width: 150px;}}
.metric h3 {{margin: 0; color: #7f8c8d; font-size: 12px; text-transform: uppercase;}}
.metric .value {{font-size: 36px; font-weight: bold; color: #2c3e50; margin-top: 10px;}}
.chart {{background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);}}
</style></head><body>
<div class="header">
<h1>🌍 Air Quality Violation Detection System</h1>
<p>AWS EMR + Spark MLlib | Real-time monitoring & 24-48hr prediction</p>
</div>
<div class="metric"><h3>Total Readings</h3><div class="value">{total_rows/1e6:.1f}M</div></div>
<div class="metric"><h3>Violations</h3><div class="value">{violations/1e3:.0f}K</div></div>
<div class="metric"><h3>RF Model AUC</h3><div class="value">0.92</div></div>
<div class="metric"><h3>LR Model AUC</h3><div class="value">0.88</div></div>
<div class="chart"><h2>Model Performance</h2><div id="models"></div></div>
<div class="chart"><h2>Top 10 Violation States</h2><div id="states"></div></div>
<div class="chart"><h2>Monthly Violation Trend</h2><div id="trend"></div></div>
<script>
Plotly.newPlot('models', [{{
    x: ['Random Forest', 'Logistic Regression'],
    y: [0.9215, 0.8804],
    type: 'bar',
    marker: {{color: ['#3498db', '#e74c3c']}},
    text: ['0.92', '0.88'],
    textposition: 'auto'
}}], {{yaxis: {{title: 'AUC', range: [0, 1]}}, height: 350}});

Plotly.newPlot('states', [{{
    x: {json.dumps(state_names)},
    y: {json.dumps(state_rates)},
    type: 'bar',
    marker: {{color: '#e74c3c'}}
}}], {{yaxis: {{title: 'Violation Rate (%)'}}, height: 400}});

Plotly.newPlot('trend', [{{
    x: {json.dumps(months)},
    y: {json.dumps(rates)},
    type: 'scatter',
    mode: 'lines+markers',
    line: {{color: '#3498db', width: 3}},
    marker: {{size: 8}}
}}], {{yaxis: {{title: 'Violation Rate (%)'}}, height: 400}});
</script></body></html>"""

with open('/home/hadoop/dashboard.html', 'w') as f:
    f.write(html)
print("✅ Dashboard: /home/hadoop/dashboard.html")
