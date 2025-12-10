import streamlit as st
import plotly.graph_objects as go
import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
os.environ['PYTHONPATH'] = '/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip'
os.environ['SPARK_HOME'] = '/usr/lib/spark'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length, hour, month
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from kafka import KafkaConsumer
import json

st.set_page_config(page_title="Air Quality System", layout="wide")

# Initialize Spark once
@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("Dashboard").config("spark.driver.memory", "2g").config("spark.ui.showConsoleProgress", "false").getOrCreate()

@st.cache_resource
def load_model(_spark):
    return RandomForestClassificationModel.load("hdfs:///user/hadoop/models/rf_model")

@st.cache_data
def load_aggregates():
    spark = get_spark()
    df = spark.read.parquet("hdfs:///user/hadoop/data/features").filter((length(col("state_name")) > 5) & ~col("state_name").contains("Country"))
    states_data = df.groupBy("state_name").agg((avg("violation")*100).alias("rate")).orderBy(col("rate").desc()).limit(15).collect()
    return [(r['state_name'], round(r['rate'],2)) for r in states_data]

spark = get_spark()
model = load_model(spark)
states_agg = load_aggregates()
all_states = [s[0] for s in states_agg]

st.title("🌍 Air Quality Violation Detection System")
st.markdown("**AWS EMR + Spark MLlib + Kafka**")

# Kafka Stream
st.sidebar.header("🔴 Live Kafka Stream")
if st.sidebar.button("Start Stream"):
    consumer = KafkaConsumer('air_quality', bootstrap_servers='localhost:9092',
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                            auto_offset_reset='latest', consumer_timeout_ms=10000)
    placeholder = st.sidebar.empty()
    for i, msg in enumerate(consumer):
        data = msg.value
        placeholder.success(f"**Alert #{i+1}**\n{data['state']}\nPM2.5: {data['pm25']:.1f}")
        if i >= 9:
            break

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Readings", "42.2M")
col2.metric("Violations", "442K")
col3.metric("RF AUC", "0.9215")
col4.metric("LR AUC", "0.8804")

st.subheader("📊 Model Performance")
fig1 = go.Figure(go.Scatter(x=['Random Forest', 'Logistic Regression'], y=[0.9215, 0.8804], 
                            mode='markers+lines', marker=dict(size=20, color=['#667eea', '#764ba2']), 
                            line=dict(dash='dash', width=2)))
fig1.update_layout(yaxis=dict(range=[0.8,1], title='AUC'), height=300)
st.plotly_chart(fig1, use_container_width=True)

st.subheader("🗺️ Top 10 Violation States")
top_states = [s[0] for s in states_agg[:10]]
rates = [s[1] for s in states_agg[:10]]
fig2 = go.Figure(go.Bar(x=rates, y=top_states, orientation='h', 
                        marker=dict(color=rates, colorscale='RdYlGn_r', showscale=True),
                        text=[f"{r}%" for r in rates], textposition='outside'))
fig2.update_layout(xaxis_title="Violation Rate (%)", height=400, yaxis=dict(autorange="reversed"))
st.plotly_chart(fig2, use_container_width=True)

st.sidebar.header("🔮 Prediction")
pred_state = st.sidebar.selectbox("State", all_states)
scenario = st.sidebar.radio("Scenario", ["Current", "Wildfire"])

if st.sidebar.button("Predict 24hrs"):
    df = spark.read.parquet("hdfs:///user/hadoop/data/features").filter((length(col("state_name")) > 5) & ~col("state_name").contains("Country"))
    if scenario == "Current":
        state_df = df.filter(col("state_name")==pred_state).orderBy(col("timestamp_local").desc()).limit(24)
    else:
        state_df = df.filter((col("state_name")==pred_state) & (col("month").isin([8,9]))).orderBy(col("pm25").desc()).limit(24)
    
    assembler = VectorAssembler(inputCols=["pm25_lag_24h", "pm25_lag_48h", "pm25_avg_7d", "pm25_std_7d", "hour", "day_of_week", "month"], outputCol="features")
    preds = model.transform(assembler.transform(state_df))
    viols = preds.filter(col("prediction")==1).count()
    st.sidebar.success(f"**{viols}/24 hours**\nviolations")
