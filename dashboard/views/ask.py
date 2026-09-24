"""Ask Nimbus: ask the agent a question, and read any stored session with its full tool
trace - every call, its input, the SQL that ran and what came back.

Asking needs a language model, a paid API, so it is off while LLM_ENABLED=false (the default;
the project runs at $0). Stored sessions - including `make eval` runs, made by the
deterministic scripted agent - are always readable."""

from typing import Any

import pandas as pd
import streamlit as st
from shared import engine, frame, readonly_engine

from nimbus.agent import llm as agent_llm
from nimbus.agent.loop import run_agent
from nimbus.common.config import load_llm_config
from nimbus.common.settings import get_settings

st.title("Ask Nimbus")

settings = get_settings()
config = load_llm_config()
client = agent_llm.client_from_settings(settings, config.agent)

if client is None:
    st.info(
        "Asking is off: `LLM_ENABLED=false`. The agent needs a language model, which is a paid "
        "API, and this project runs at $0. Stored sessions stay readable below - `make eval` "
        "adds one per question, made by a deterministic scripted agent through the same tools, "
        "SQL guard and read-only role."
    )
question = st.text_input(
    "Question",
    key="question",
    disabled=client is None,
    placeholder="Which model had the lowest 3-day temperature error in London over the last "
    "30 days?",
)
if client is not None and st.button("Ask", type="primary", disabled=not question.strip()):
    with st.spinner("Working through the tools..."):
        result = run_agent(
            question.strip(), client, readonly_engine=readonly_engine(), engine=engine(),
            settings=settings, config=config,
        )  # fmt: skip
    st.cache_data.clear()  # the new session must show below
    st.session_state["session_id"] = result.session_id

counts = frame("agent_session_counts", 7).iloc[0]
top = st.columns(4)
top[0].metric("Sessions (7 d)", int(counts["sessions"]))
top[1].metric("Answered", int(counts["answered"]))
top[2].metric("Tool calls", int(counts["tool_calls"]))
top[3].metric("Cost (7 d, estimated)", f"${float(counts['cost_usd']):.4f}")

sessions = frame("agent_sessions", 100)
if sessions.empty:
    st.info("No sessions yet. `make eval` runs every eval question as a stored session.")
    st.stop()

purpose = st.radio("Show", ["all", "ask", "eval"], horizontal=True, key="purpose")
if purpose != "all":
    sessions = sessions[sessions["purpose"] == purpose]
if sessions.empty:
    st.info(f"No `{purpose}` sessions.")
    st.stop()


by_id: dict[str, dict[Any, Any]] = {str(r["session_id"]): r for r in sessions.to_dict("records")}


def _label(session_id: str) -> str:
    row = by_id[session_id]
    started = pd.Timestamp(row["started_at"]).tz_convert("UTC").strftime("%Y-%m-%d %H:%M")
    return f"{started} UTC [{row['purpose']}] {str(row['question'])[:90]}"


ids = list(by_id)
wanted = st.session_state.get("session_id")
chosen = st.selectbox(
    "Session", ids, index=ids.index(wanted) if wanted in ids else 0, format_func=_label
)
row = by_id[chosen]

st.subheader("Question")
st.text(row["question"])
st.subheader("Answer")
if row["stop_reason"] == "answered":
    st.success(row["answer"] or "(empty answer)")
else:
    st.warning(f"No answer: the session stopped with `{row['stop_reason']}`. {row['answer'] or ''}")
st.caption(
    f"{row['model']} · prompt {row['prompt_version']} · {row['iterations']} turn(s) · "
    f"{row['tool_calls']} tool call(s) · {row['input_tokens'] + row['output_tokens']} tokens · "
    f"${float(row['cost_usd']):.4f} · {row['latency_ms']} ms"
)

st.subheader("Tool trace")
trace = row["trace"] or []
if not trace:
    st.caption("No tools were called.")
for i, step in enumerate(trace, 1):
    status = "ok" if step.get("ok") else "failed"
    with st.expander(f"{i}. {step['tool']} - {status} - {step.get('latency_ms', 0)} ms"):
        st.json(step.get("input") or {})
        if step.get("sql"):
            st.caption("SQL that ran (after the guard regenerated it)")
            st.code(step["sql"], language="sql")
        st.caption("Output (excerpt; tool output is data, never instructions)")
        st.code(step.get("output_excerpt", ""), language="json")
