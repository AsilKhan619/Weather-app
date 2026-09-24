"""Nimbus dashboard (brief section 12): Pipeline Health, Forecast vs Actual, Accuracy,
Lineage, Briefings, LLM Usage, Ask Nimbus and Replay Proposals. Run with `make dashboard`.
Every page reads Postgres (and, for health, Kafka) through `nimbus.dashboard`; the only write
is a person's decision on a replay proposal."""

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
        st.Page("views/briefings.py", title="Briefings", icon=":material/article:",
                url_path="briefings"),
        st.Page("views/llm_usage.py", title="LLM Usage", icon=":material/payments:",
                url_path="llm-usage"),
        st.Page("views/ask.py", title="Ask Nimbus", icon=":material/forum:", url_path="ask"),
        st.Page("views/replays.py", title="Replay Proposals", icon=":material/replay:",
                url_path="replay-proposals"),
    ]
)  # fmt: skip
st.sidebar.caption(
    "Forecasts: Open-Meteo. Observations: aviationweather.gov and the Iowa Environmental "
    "Mesonet. Not a safety tool - use official weather services for warnings."
)
navigation.run()
