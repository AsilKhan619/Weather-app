"""Lineage: follow one event from the API request to the gold metrics it fed."""

import streamlit as st
from shared import engine

from nimbus.jobs.trace import format_trace, sample_event_id, trace_event
from nimbus.streaming.bronze_reader import DEFAULT_LAKE_ROOT

st.title("Lineage")
st.write(
    "Paste an `event_id` (it is in every structured log line) to see its API request, Kafka "
    "position, bronze file, silver rows and the gold metrics it contributed to - the same as "
    "`make trace`."
)

if "event_id" not in st.session_state:
    st.session_state["event_id"] = ""

cols = st.columns(3)
if cols[0].button("Sample forecast event"):
    st.session_state["event_id"] = sample_event_id("weather.forecast.raw.v1") or ""
if cols[1].button("Sample observation event"):
    st.session_state["event_id"] = sample_event_id("weather.observation.raw.v1") or ""

event_id = st.text_input("Event id", key="event_id").strip()
if not event_id:
    if not DEFAULT_LAKE_ROOT.exists():
        st.warning(
            f"The bronze lake ({DEFAULT_LAKE_ROOT}) does not exist here, so no event can be traced."
        )
    st.stop()

trace = trace_event(engine(), event_id)
if not trace.bronze:
    st.error("Not found in the bronze lake - never consumed by the bronze sink, or a different id.")
    st.stop()

first = trace.bronze[0]
metrics = st.columns(4)
metrics[0].metric("Kafka position", f"p{first.partition} @ {first.offset}")
metrics[1].metric("Silver rows from this event", f"{trace.silver_expected:,}")
metrics[2].metric("Still carrying its id", f"{trace.silver_current:,}")
metrics[3].metric("Gold verification rows", f"{trace.gold_verification:,}")

if trace.silver_current < trace.silver_expected:
    st.info(
        "Some of this event's rows were overwritten by a later event (a correction or an "
        "overlapping backfill window); the trace still finds them by their natural keys."
    )
if trace.gold_accuracy:
    st.subheader("Daily accuracy rows it contributed to")
    st.dataframe(trace.gold_accuracy, hide_index=True, width="stretch")
st.subheader("Full trace")
st.code(format_trace(trace), language="text")
