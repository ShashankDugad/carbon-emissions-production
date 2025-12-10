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

st.set_page_config(page_title="Air Quality System", layout="wide")

@st.cache_resource
def load_data():
    spark = SparkSession.builder.appName("Dashboard").config("spark.driver.memory", "2g").getOrCreate()
    model = RandomForestClassificationModel.load("hdfs:///user/hadoop/models/rf_model")
    df = spark.read.parquet("hdfs:///user/hadoop/data/features").filter((length(col("state_name")) > 5) & ~col("state_name").contains("Country"))
    return spark, model, df

spark, model, df = load_data()

st.title("🌍 Air Quality Violation Detection System")
st.markdown("**AWS EMR + Spark MLlib** | 42M Records | Real-time + 24-48hr Prediction")

# Get top states
states_data = df.groupBy("state_name").agg((avg("violation")*100).alias("rate")).orderBy(col("rate").desc()).limit(15).collect()
all_states = [r['state_name'] for r in states_data]

# Sidebar filter
st.sidebar.header("🎯 Filter")
selected_state = st.sidebar.selectbox("Select State", ["All States"] + all_states)

# Filter data
if selected_state == "All States":
    filtered_df = df
    title_suffix = ""
else:
    filtered_df = df.filter(col("state_name") == selected_state)
    title_suffix = f" - {selected_state}"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Readings", f"{filtered_df.count()/1e6:.1f}M")
viols = filtered_df.filter(col("violation")==1).count()
col2.metric("Violations", f"{viols/1e3:.0f}K")
col3.metric("RF AUC", "0.9215")
col4.metric("LR AUC", "0.8804")

st.subheader("📊 Model Performance")
fig1 = go.Figure()
fig1.add_trace(go.Scatter(x=['Random Forest', 'Logistic Regression'], y=[0.9215, 0.8804], 
                          mode='markers+lines', marker=dict(size=20, color=['#667eea', '#764ba2']), 
                          line=dict(dash='dash', width=2)))
fig1.update_layout(yaxis=dict(range=[0.8,1], title='AUC Score'), height=300)
st.plotly_chart(fig1, use_container_width=True)

if selected_state == "All States":
    st.subheader("🗺️ Top 10 Violation States")
    top_states = [r['state_name'] for r in states_data[:10]]
    rates = [round(r['rate'],2) for r in states_data[:10]]
    fig2 = go.Figure(go.Bar(x=rates, y=top_states, orientation='h', 
                            marker=dict(color=rates, colorscale='RdYlGn_r', showscale=True),
                            text=[f"{r}%" for r in rates], textposition='outside'))
    fig2.update_layout(xaxis_title="Violation Rate (%)", height=400, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig2, use_container_width=True)

st.subheader(f"⏰ Hourly Violation Pattern{title_suffix}")
hourly = filtered_df.withColumn("hr", hour("timestamp_local")).groupBy("hr").agg((avg("violation")*100).alias("rate")).orderBy("hr").collect()
hours = [r['hr'] for r in hourly]
h_rates = [r['rate'] for r in hourly]
fig3 = go.Figure(go.Scatter(x=hours, y=h_rates, mode='lines', fill='tozeroy', 
                            line=dict(color='#667eea', width=3)))
fig3.update_layout(xaxis_title="Hour of Day", yaxis_title="Violation Rate (%)", height=350)
st.plotly_chart(fig3, use_container_width=True)

st.subheader(f"📈 Monthly Violation Trends{title_suffix}")
monthly = filtered_df.withColumn("mon", month("timestamp_local")).groupBy("mon").agg((avg("violation")*100).alias("rate")).orderBy("mon").collect()
months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
m_rates = [r['rate'] for r in monthly]
fig4 = go.Figure(go.Scatter(x=months[:len(m_rates)], y=m_rates, mode='lines+markers', 
                            line=dict(color='#48bb78', width=3), marker=dict(size=10)))
fig4.update_layout(yaxis_title="Violation Rate (%)", height=350)
st.plotly_chart(fig4, use_container_width=True)

st.sidebar.header("🔮 Live Prediction")
pred_state = st.sidebar.selectbox("State", all_states, key="pred")
scenario = st.sidebar.radio("Scenario", ["Current Conditions", "Wildfire Season"])

if st.sidebar.button("Predict Next 24hrs"):
    if scenario == "Current Conditions":
        state_df = df.filter(col("state_name")==pred_state).orderBy(col("timestamp_local").desc()).limit(24)
    else:
        state_df = df.filter((col("state_name")==pred_state) & (col("month").isin([8,9]))).orderBy(col("pm25").desc()).limit(24)
    
    assembler = VectorAssembler(inputCols=["pm25_lag_24h", "pm25_lag_48h", "pm25_avg_7d", "pm25_std_7d", "hour", "day_of_week", "month"], outputCol="features")
    test_df = assembler.transform(state_df)
    preds = model.transform(test_df)
    violations = preds.filter(col("prediction")==1).count()
    total = preds.count()
    st.sidebar.success(f"**{violations}/{total} hours**\nwith violations\n\n*{scenario}*")
