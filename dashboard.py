import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="SQL → NoSQL Migration Dashboard", layout="wide")

st.title("SQL → NoSQL Cloud Migration Decision Dashboard")

# Load summary

with open("output/migration_summary.json") as f:
    summary = json.load(f)

st.header("Migration Recommendation")

col1, col2, col3 = st.columns(3)

col1.metric("Recommended NoSQL Model", summary["recommendation"]["nosql_model"])
col2.metric("Confidence", f"{summary['recommendation']['confidence']*100:.1f}%")
col3.metric("Target Database", summary["recommendation"]["target_db"])

st.header("Schema Statistics")

schema_data = pd.DataFrame(summary["schema_stats"]).T
st.dataframe(schema_data)

st.header("Workload Characteristics")

col1, col2, col3 = st.columns(3)

col1.metric("Total Queries", summary["workload"]["total_queries"])
col2.metric("Read/Write Ratio", summary["workload"]["read_write_ratio"])
col3.metric("Join Ratio", summary["workload"]["join_ratio"])

st.header("Cost Estimation")

cost_data = pd.DataFrame({
"Metric": ["Storage (GB)", "Monthly Cost ($)", "RDS Baseline ($)", "Savings (%)"],
"Value": [
summary["cost"]["storage_gb"],
summary["cost"]["monthly_usd"],
summary["cost"]["rds_baseline_usd"],
summary["cost"]["savings_pct"]
]
})

st.table(cost_data)

st.header("Collections Generated")

collections = summary["collections"]
st.write(collections)

st.header("Cloud Cost Comparison")

cloud_costs = {
"AWS": 0.0296,
"Azure": 0.0294,
"GCP": 0.0280,
"DigitalOcean": 0.0030
}

df = pd.DataFrame(list(cloud_costs.items()), columns=["Provider","Cost"])

st.dataframe(df)

fig, ax = plt.subplots()
ax.bar(df["Provider"], df["Cost"])
ax.set_ylabel("Monthly Cost ($)")
ax.set_title("Cloud Provider Cost Comparison")

st.pyplot(fig)

best = df.loc[df["Cost"].idxmin()]

st.success(f"Best Cloud Provider: {best['Provider']} (${best['Cost']:.4f}/month)")
