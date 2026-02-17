import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Medical BI Dashboard", layout="wide")

def get_abs_path(rel_path):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), rel_path))

DATA_FILE = get_abs_path("data/raw/processed_data.csv")
PLOT_GLOBAL = get_abs_path("data/results/shap_summary_plot.png")
PLOT_LOCAL = get_abs_path("data/results/shap_local_prediction.png")

st.title("🏥 Ethiopian Medical Business Intelligence")

# 1. Data Filter
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE)
    st.sidebar.header("Filters")
    if 'channel_name' in df.columns:
        channels = ["All"] + list(df['channel_name'].unique())
        choice = st.sidebar.selectbox("Channel Name", channels)
        df_display = df if choice == "All" else df[df['channel_name'] == choice]
    else:
        df_display = df

    st.subheader("📋 Scraped Data Overview")
    st.dataframe(df_display, width='stretch')

st.divider()

# 2. Visuals
st.subheader("🧠 Model Explainability")
col1, col2 = st.columns(2)
with col1:
    st.write("**Global Importance**")
    if os.path.exists(PLOT_GLOBAL): st.image(PLOT_GLOBAL, width='stretch')
with col2:
    st.write("**Local Explanation (Sample 1)**")
    if os.path.exists(PLOT_LOCAL): st.image(PLOT_GLOBAL, width='stretch')