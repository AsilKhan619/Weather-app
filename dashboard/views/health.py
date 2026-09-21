"""Pipeline Health: is data flowing, is anything behind, is anything wrong?"""

import pandas as pd
import streamlit as st
from shared import engine, frame, line_chart

from nimbus.common.settings import get_settings
from nimbus.dashboard.kafka_health import kafka_health
from nimbus.quality.freshness import check_freshness

st.title("Pipeline health")


@st.cache_data(ttl=15, show_spinner=False)
def _kafka() -> tuple[pd.DataFrame, int | None, str | None]:
    health = kafka_health(get_settings())
    return health.lag, health.dlq_messages, health.error


@st.cache_data(ttl=60, show_spinner=False)
def _freshness() -> pd.DataFrame:
    results = check_freshness(engine())
    return pd.DataFrame(
        {
            "table": [r.table for r in results],
            "subject": [r.subject for r in results],
            "status": ["stale" if not r.passed else "ok" for r in results],
            "detail": [r.detail for r in results],
        }
    )


lag, dlq, kafka_error = _kafka()
fresh = _freshness()
alerts_24h = frame("alert_counts", 24)
quality = frame("quality_summary", 7)

top = st.columns(4)
top[0].metric("Alerts (24 h)", int(alerts_24h["alerts"].sum()) if not alerts_24h.empty else 0)
top[1].metric("DLQ messages", "n/a" if dlq is None else f"{dlq:,}")
top[2].metric("Max consumer lag", "n/a" if lag.empty else f"{int(lag['lag'].max()):,}")
top[3].metric("Stale sources", int((fresh["status"] == "stale").sum()))

st.subheader("Throughput")
throughput = frame("hourly_throughput", 24)
if throughput.empty:
    st.info("No silver rows were written or changed in the last 24 hours.")
else:
    throughput["hour"] = pd.to_datetime(throughput["hour"], utc=True)
    st.altair_chart(line_chart(throughput, "hour", "rows", "table_name", "rows / hour"))

st.subheader("Consumer lag and dead letters")
if kafka_error:
    st.warning(f"Kafka is not reachable, so lag and DLQ volume are unavailable. ({kafka_error})")
else:
    st.dataframe(lag, hide_index=True, width="stretch")
    st.caption(
        "Lag = end offset - committed offset. A drained pipeline shows 0; a consumer that has "
        "never committed shows its whole backlog."
    )

st.subheader("Freshness")
stale = fresh[fresh["status"] == "stale"]
if stale.empty:
    st.success("Every station and live source is within its expected interval.")
else:
    st.warning(f"{len(stale)} of {len(fresh)} stations and sources have no recent data.")
st.dataframe(fresh.sort_values(["status", "subject"], ascending=[False, True]), hide_index=True,
             width="stretch")  # fmt: skip

st.subheader("Data quality (last 7 days)")
if quality.empty:
    st.info("No quality results yet - run `make gold` or `make quality`.")
else:
    st.dataframe(quality, hide_index=True, width="stretch")
    failures = frame("quality_failures", 7, 50)
    if failures.empty:
        st.success("No failed checks.")
    else:
        st.caption(
            "Failed checks, newest first (blocking = message or day rejected; warning = kept)."
        )
        st.dataframe(failures, hide_index=True, width="stretch")

st.subheader("Reconciliation")
reconciliation = frame("latest_reconciliation")
if reconciliation.empty:
    st.info("`make reconcile` has not run yet.")
else:
    st.dataframe(reconciliation, hide_index=True, width="stretch")

st.subheader("Alerts")
alerts = frame("recent_alerts", 50)
if alerts.empty:
    st.info(
        "No alerts yet. The detector needs two consecutive live runs of a model (run-change) or "
        "live forecasts for an observation's hour; backfilled history never alerts."
    )
else:
    st.dataframe(alerts.drop(columns=["alert_id"]), hide_index=True, width="stretch")

st.subheader("Ingestion runs")
runs = frame("ingestion_runs", 20)
if runs.empty:
    st.info("No ingestion runs recorded.")
else:
    st.dataframe(runs, hide_index=True, width="stretch")
