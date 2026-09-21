"""Nimbus dashboard (brief section 12), v1: Pipeline Health, Forecast vs Actual, Accuracy and
Lineage. Run with `make dashboard`. Every page reads Postgres (and, for health, Kafka)
through `nimbus.dashboard` and is read-only."""

import streamlit as st

st.set_page_config(page_title="Nimbus", page_icon=":material/cloud:", layout="wide")

navigation = st.navigation(
    [
        st.Page("views/health.py", title="Pipeline Health", icon=":material/monitor_heart:",
                url_path="health", default=True),
        st.Page("views/forecast_vs_actual.py", title="Forecast vs Actual",
                icon=":material/show_chart:", url_path="forecast-vs-actual"),
        st.Page("views/accuracy.py", title="Accuracy", icon=":material/leaderboard:",
                url_path="accuracy"),
        st.Page("views/lineage.py", title="Lineage", icon=":material/account_tree:",
                url_path="lineage"),
    ]
)  # fmt: skip
st.sidebar.caption(
    "Forecasts: Open-Meteo. Observations: aviationweather.gov and the Iowa Environmental "
    "Mesonet. Not a safety tool - use official weather services for warnings."
)
navigation.run()
