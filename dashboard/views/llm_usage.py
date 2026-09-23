"""LLM Usage: what the briefings cost, how often the cache saved a call, how often it failed."""

import pandas as pd
import streamlit as st
from shared import frame

from nimbus.common.settings import get_settings

st.title("LLM usage")

by_outcome = frame("llm_usage_by_outcome", 30)
if by_outcome.empty:
    st.info("No LLM requests logged yet. Every call, cache hit and skip lands in `ops.llm_calls`.")
    st.stop()

api = by_outcome[~by_outcome["outcome"].isin(["cache_hit", "disabled"])]
cache_hits = int(by_outcome.loc[by_outcome["outcome"] == "cache_hit", "calls"].sum())
api_calls = int(api["calls"].sum())
top = st.columns(4)
top[0].metric("Cost (30 d, estimated)", f"${by_outcome['cost_usd'].sum():.4f}")
top[1].metric("API calls", api_calls)
top[2].metric(
    "Cache hit rate",
    "n/a" if api_calls + cache_hits == 0 else f"{cache_hits / (api_calls + cache_hits):.0%}",
)
latency = api["avg_latency_ms"].dropna()
top[3].metric("Avg latency", "n/a" if latency.empty else f"{latency.mean():.0f} ms")
if not get_settings().llm_enabled:
    st.caption("The LLM is off (`LLM_ENABLED=false`): requests are logged as `disabled`.")

st.subheader("By model and outcome (30 days)")
st.dataframe(by_outcome, hide_index=True, width="stretch")
st.caption(
    "Cost is estimated from token counts and the list prices in `config/llm.yaml`; the bill is "
    "the source of truth. `invalid_output` is retried once; `grounding_failed` is stored flagged."
)

daily = frame("llm_usage_daily", 30)
if not daily.empty:
    st.subheader("Daily")
    daily["day"] = pd.to_datetime(daily["day"], utc=True)
    st.bar_chart(daily.set_index("day")[["api_calls", "cache_hits"]])

st.subheader("Recent requests")
st.dataframe(frame("recent_llm_calls", 50), hide_index=True, width="stretch")
