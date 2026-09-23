"""Briefings: the latest LLM briefing per location, with what it was allowed to say."""

import pandas as pd
import streamlit as st
from shared import frame

from nimbus.common.settings import get_settings

st.title("Briefings")

counts = frame("briefing_counts", 7)
briefings = frame("latest_briefings")
llm_on = get_settings().llm_enabled

if briefings.empty:
    if llm_on:
        st.info("No briefings yet - run `make briefings` (daily) or `make briefing-consumer`.")
    else:
        st.info(
            "No briefings: the LLM is off (`LLM_ENABLED=false`, the default), so the pipeline "
            "builds each fact sheet and logs the skip without calling an API. Set a key and "
            "`LLM_ENABLED=true` to generate them; `make briefings ARGS=--dry-run` shows the facts."
        )
    st.stop()

row = counts.iloc[0]
top = st.columns(4)
top[0].metric("Briefings (7 d)", int(row["briefings"]))
top[1].metric("Grounded", int(row["grounded"]))
top[2].metric("Flagged (never published)", int(row["flagged"]))
top[3].metric("Published", int(row["published"]))

st.caption(
    "Every number in a briefing must appear in, or round from, its fact sheet; the confidence "
    "level and most reliable model are decided by code. A briefing that fails is stored and "
    "flagged, and never published."
)

names = briefings["location"].fillna(briefings["location_id"])
place: str | None = st.selectbox("Location", names)
brief = briefings.loc[names == place].iloc[0]

if brief["grounding_passed"]:
    st.success(f"Grounded - confidence **{brief['confidence']}**")
else:
    st.error("Flagged by the grounding check - not published:")
    for failure in brief["grounding_failures"]:
        st.write(f"- {failure}")

st.subheader(brief["headline"])
st.write(brief["summary"])
st.markdown(f"**Most reliable model:** {brief['most_reliable_model']} - {brief['reliable_reason']}")
if len(brief["notable_risks"]):
    st.markdown("**Notable risks:**")
    for risk in brief["notable_risks"]:
        st.write(f"- {risk}")

as_of = pd.Timestamp(brief["as_of"]).tz_convert("UTC")
st.caption(
    f"As of {as_of:%Y-%m-%d %H:%M} UTC - {brief['trigger']} trigger - {brief['model']} "
    f"(prompt {brief['prompt_version']}) - "
    + ("published" if pd.notna(brief["published_at"]) else "not published")
)
with st.expander("Fact sheet (the only input the model saw)"):
    st.json(brief["fact_sheet"])
