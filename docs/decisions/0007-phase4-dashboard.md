# ADR 0007: Dashboard v1 (Phase 4b)

**Status:** Accepted
**Date:** 2026-09-21

## Context

Brief section 12 lists eight pages; Phase 4 delivers four: **Pipeline Health**, **Forecast vs
Actual**, **Accuracy** and **Lineage**. Briefings, LLM usage, Ask Nimbus and Replay Proposals
belong to Phases 5 and 6. Acceptance: the pages work against real data.

## Decisions

- **A query layer with no Streamlit in it.** `src/nimbus/dashboard/queries.py` is plain
  functions from an `Engine` to DataFrames, and `kafka_health.py` does the same for Kafka. The
  pages in `dashboard/views/` are thin: widgets, a chart, a table. That split is what makes the
  dashboard testable - the queries are integration-tested against a real Postgres with known
  numbers (a leaderboard that must rank ICON first, an error curve that must rise with lead time),
  and each page is rendered headlessly with Streamlit's `AppTest`.
- **`st.navigation` with file-based pages**, so `AppTest.switch_page` can drive each page and a
  page can be read on its own.
- **Read-only by construction.** Every query is a `SELECT`. The dashboard uses the ordinary
  connection for now; the read-only role arrives with the agent in Phase 6 and the dashboard
  should move to it then (noted, not done).
- **Display units are converted in one place** (`queries.display`, `display_error`): stored SI
  values (K, Pa, m/s) become degC and hPa on the way out. An *error* is scaled but never offset -
  a 2 K error is 2 degC, not -271 degC - and there is a test for exactly that.
- **Accuracy numbers come from `gold.accuracy_daily`, not from re-aggregating raw errors**,
  combined by weighting with the verified count. That is exact (ADR 0005) and cheap: the
  leaderboard scans a few thousand daily rows rather than millions of verified forecasts.
  Forecast vs Actual is the one page that reads `gold.forecast_verification`, and only for one
  location, variable, lead day and date range.
- **Short-TTL caching** (`st.cache_data`, 30 s; Kafka 15 s) so the page follows a running
  pipeline without a query per widget click. The engine is a `cache_resource` singleton.
- **Health survives Kafka being down.** `kafka_health` turns every failure into a message, and the
  page shows "Kafka is not reachable" instead of an error: the data already in Postgres is still
  worth looking at, and there is a test with an unreachable broker. Consumer lag is
  end offset - committed offset per group and topic; a group that has never committed shows its
  whole backlog.
- **Freshness is reported as it is**: with the live producers stopped, every station and source is
  stale. That is the right answer, not a dashboard bug.
- **Lineage reuses `make trace`** (`trace_event`, `format_trace`), so the page and the CLI cannot
  disagree. It needs the bronze lake on local disk (`data/lake/bronze`) and says so when it is
  missing.
- **No screenshots yet.** Phase 8 (portfolio polish) is where they belong; what Phase 4 proves is
  that the pages render and are correct.

## What is not verified

- The pages were exercised with Streamlit's headless test runner against seeded data, an empty
  database, and the live-demo run's real data. **Nobody has looked at them in a browser in this
  session**, so layout and chart readability are unchecked beyond "renders without error".
- Forecast vs Actual matches on the nearest observation within 30 minutes and shows only hours
  that have one; a station that reports sparsely produces a sparse line by design.
- Streamlit reruns the whole page script on each widget change; with the current data volume the
  queries take milliseconds to a couple of seconds, but the Accuracy page runs three queries per
  rerun and has not been load-tested against a full history.
