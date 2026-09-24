"""Replay Proposals: the agent can only propose re-consuming a topic from a point in time; a
person decides here. Approving records the decision and shows the runbook command - it runs
nothing, because a replay needs the consumer stopped first (runbook section 3).

The one page that writes: it records a decision in ops.replay_proposals. Who decided is
self-reported - there is no login on this local dashboard (ADR 0009)."""

import pandas as pd
import streamlit as st
from shared import engine, frame

from nimbus.agent.proposals import DecisionError, decide, replay_command

st.title("Replay proposals")

if flash := st.session_state.pop("replay_flash", None):
    st.success(flash)

pending = frame("replay_proposals", "pending")
st.subheader(f"Pending ({len(pending)})")
if pending.empty:
    st.info(
        "Nothing is waiting. The agent files a proposal with `propose_replay` when data in the "
        "last 7 days was lost or loaded wrongly; each one waits here for a person."
    )

for p in pending.to_dict("records"):
    pid = int(p["id"])
    from_time = pd.Timestamp(p["from_time"]).tz_convert("UTC")
    with st.container(border=True):
        st.markdown(
            f"**#{pid}** - re-read `{p['topic']}` for consumer group `{p['consumer_group']}` "
            f"from **{from_time:%Y-%m-%d %H:%M} UTC**"
        )
        # Written by the agent from untrusted inputs: shown as plain text, never as markdown.
        st.text(f"Reason: {p['reason']}")
        if isinstance(p["question"], str):
            st.text(f"Asked: {p['question']}")
        proposed_at = pd.Timestamp(p["proposed_at"]).tz_convert("UTC")
        st.caption(f"Proposed by {p['proposed_by']} at {proposed_at:%Y-%m-%d %H:%M} UTC")
        command = replay_command(p["consumer_group"], p["topic"], from_time.to_pydatetime())
        st.code(command, "bash")
        with st.form(key=f"decide-{pid}"):
            who = st.text_input("Your name", key=f"who-{pid}")
            note = st.text_input("Note (optional)", key=f"note-{pid}")
            left, right = st.columns(2)
            approve = left.form_submit_button("Approve", type="primary")
            reject = right.form_submit_button("Reject")
        if approve or reject:
            try:
                status = decide(engine(), pid, approve=approve, decided_by=who, note=note)
            except DecisionError as exc:
                st.error(str(exc))
            else:
                st.cache_data.clear()
                st.session_state["replay_flash"] = f"Proposal #{pid} {status}." + (
                    " Nothing has run yet: stop the consumer, run the command, restart it, then "
                    "`make reconcile` (runbook section 3)."
                    if approve
                    else ""
                )
                st.rerun()

st.subheader("Decided")
decided = pd.concat(
    [frame("replay_proposals", "approved"), frame("replay_proposals", "rejected")],
    ignore_index=True,
)
if decided.empty:
    st.caption("No decisions yet.")
else:
    st.dataframe(
        decided.sort_values("decided_at", ascending=False)[
            ["id", "status", "decided_at", "decided_by", "decision_note", "consumer_group",
             "topic", "from_time", "reason", "proposed_at"]
        ],
        hide_index=True,
        width="stretch",
    )  # fmt: skip
