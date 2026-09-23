"""Everything the dashboard reads, as plain functions from an Engine to DataFrames.

No Streamlit here: the pages are thin (widgets + charts) and this module is what is
tested against a real Postgres. All queries are read-only. Values are stored in SI (K, Pa,
m/s) and converted for display by `display` (degC, hPa, m/s), so the pages never do unit
arithmetic themselves."""

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from nimbus.common.units import (  # re-exported: the pages import them from here
    VARIABLE_LABELS,
    display,
    display_error,
    display_unit,
)

__all__ = ["VARIABLE_LABELS", "display", "display_error", "display_unit"]


def _read(engine: Engine, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


# --- shared lookups ----------------------------------------------------------------------


def locations(engine: Engine) -> pd.DataFrame:
    return _read(
        engine, "SELECT location_id, name, climate, station FROM silver.dim_location ORDER BY name"
    )


def variables(engine: Engine) -> pd.DataFrame:
    return _read(engine, "SELECT variable, unit, description FROM silver.dim_variable ORDER BY 1")


def verified_range(engine: Engine) -> tuple[date, date] | None:
    """First and last valid date that has gold results, or None on an empty gold layer."""
    frame = _read(
        engine, "SELECT min(valid_date) AS lo, max(valid_date) AS hi FROM gold.accuracy_daily"
    )
    lo, hi = frame.iloc[0]["lo"], frame.iloc[0]["hi"]
    if pd.isna(lo) or pd.isna(hi):
        return None
    return lo, hi


# --- pipeline health ---------------------------------------------------------------------


def hourly_throughput(engine: Engine, hours: int = 24) -> pd.DataFrame:
    """Silver rows written or changed per hour, by table, for the last `hours` hours."""
    return _read(
        engine,
        """
        SELECT date_trunc('hour', updated_at) AS hour, 'forecast' AS table_name, count(*) AS rows
        FROM silver.forecast WHERE updated_at >= now() - make_interval(hours => :h) GROUP BY 1
        UNION ALL
        SELECT date_trunc('hour', updated_at), 'observation', count(*)
        FROM silver.observation WHERE updated_at >= now() - make_interval(hours => :h) GROUP BY 1
        ORDER BY 1
        """,
        {"h": hours},
    )


def ingestion_runs(engine: Engine, limit: int = 20) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT id, source, ingestion_mode, status, started_at, finished_at, "
        "messages_produced AS produced, messages_failed AS failed, error_message "
        "FROM ops.ingestion_runs ORDER BY id DESC LIMIT :n",
        {"n": limit},
    )


def latest_reconciliation(engine: Engine) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT DISTINCT ON (topic) topic, checked_at, matched, produced_messages AS produced, "
        "bronze_messages AS bronze, bronze_unparseable AS unparseable, "
        "expected_silver_rows AS expected, silver_rows AS actual, "
        "missing_from_silver AS missing, extra_in_silver AS extra "
        "FROM ops.reconciliation_results ORDER BY topic, id DESC",
    )


def quality_failures(engine: Engine, days: int = 7, limit: int = 50) -> pd.DataFrame:
    """Failed checks of the last `days` days; the per-severity summary rows are left out."""
    return _read(
        engine,
        "SELECT checked_at, context, table_name, check_name, severity, subject, "
        "rows_failed, rows_checked, left(detail, 160) AS sample "
        "FROM ops.quality_results WHERE NOT passed AND check_name <> 'all_checks' "
        "AND checked_at >= now() - make_interval(days => :d) "
        "ORDER BY id DESC LIMIT :n",
        {"d": days, "n": limit},
    )


def quality_summary(engine: Engine, days: int = 7) -> pd.DataFrame:
    """Checks that ran, and how many of them failed, per table and severity."""
    return _read(
        engine,
        "SELECT table_name, severity, count(*) FILTER (WHERE check_name = 'all_checks') AS runs, "
        "count(*) FILTER (WHERE check_name = 'all_checks' AND NOT passed) AS failing_runs, "
        "coalesce(sum(rows_failed) FILTER (WHERE check_name = 'all_checks'), 0) AS rows_failed "
        "FROM ops.quality_results WHERE checked_at >= now() - make_interval(days => :d) "
        "AND check_name <> 'freshness' GROUP BY 1, 2 ORDER BY 1, 2",
        {"d": days},
    )


def recent_alerts(engine: Engine, limit: int = 50) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT detected_at, rule, severity, location_id, coalesce(model, station) AS subject, "
        "variable, metric, threshold, published_at IS NOT NULL AS published, alert_id "
        "FROM gold.alert ORDER BY detected_at DESC LIMIT :n",
        {"n": limit},
    )


def alert_counts(engine: Engine, hours: int = 24) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT rule, severity, count(*) AS alerts FROM gold.alert "
        "WHERE detected_at >= now() - make_interval(hours => :h) GROUP BY 1, 2 ORDER BY 1, 2",
        {"h": hours},
    )


# --- forecast vs actual ------------------------------------------------------------------


def forecast_vs_actual(
    engine: Engine, location_id: str, variable: str, lead_day: int, start: date, end: date
) -> pd.DataFrame:
    """Verified points for one location and variable: each model's forecast at the chosen
    lead and the observation it was scored against, by valid time (UTC), in display units.
    Columns: valid_time, model, forecast, observed."""
    frame = _read(
        engine,
        "SELECT valid_time, model, forecast_value, observed_value "
        "FROM gold.forecast_verification "
        "WHERE location_id = :loc AND variable = :var AND lead_day = :lead "
        "AND valid_time >= :start AND valid_time < :end ORDER BY valid_time, model",
        {
            "loc": location_id,
            "var": variable,
            "lead": lead_day,
            "start": _utc_start(start),
            "end": _utc_start(end + timedelta(days=1)),
        },
    )
    if frame.empty:
        return pd.DataFrame(columns=["valid_time", "model", "forecast", "observed"])
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True)
    frame["forecast"] = display(variable, frame.pop("forecast_value"))
    frame["observed"] = display(variable, frame.pop("observed_value"))
    return frame


def _utc_start(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


# --- accuracy ----------------------------------------------------------------------------


def leaderboard(
    engine: Engine, variable: str, window_days: int, lead_day: int, location_id: str | None
) -> pd.DataFrame:
    """Models ranked by MAE over the window ending at the latest verified date. Combining
    the daily rows by weighting with n is exact (ADR 0005). Columns are in display units."""
    frame = _read(
        engine,
        """
        WITH bounds AS (SELECT max(valid_date) AS hi FROM gold.accuracy_daily)
        SELECT a.model,
               sum(a.n) AS n,
               sum(a.n * a.bias) / sum(a.n) AS bias,
               sum(a.n * a.mae) / sum(a.n) AS mae,
               sqrt(sum(a.n * a.rmse * a.rmse) / sum(a.n)) AS rmse
        FROM gold.accuracy_daily a, bounds b
        WHERE a.variable = :var AND a.lead_day = :lead
          AND a.valid_date > b.hi - :window
          AND (CAST(:loc AS text) IS NULL OR a.location_id = :loc)
        GROUP BY a.model ORDER BY mae
        """,
        {"var": variable, "lead": lead_day, "window": window_days, "loc": location_id},
    )
    for column in ("bias", "mae", "rmse"):
        frame[column] = display_error(variable, frame[column])
    frame.insert(0, "rank", range(1, len(frame) + 1))
    return frame


def error_by_lead(
    engine: Engine, variable: str, window_days: int, location_id: str | None
) -> pd.DataFrame:
    """MAE per model at each lead day over the window: the error-versus-lead-time curve.
    Columns: lead_day, model, mae (display units), n."""
    frame = _read(
        engine,
        """
        WITH bounds AS (SELECT max(valid_date) AS hi FROM gold.accuracy_daily)
        SELECT a.lead_day, a.model, sum(a.n) AS n, sum(a.n * a.mae) / sum(a.n) AS mae
        FROM gold.accuracy_daily a, bounds b
        WHERE a.variable = :var AND a.valid_date > b.hi - :window
          AND (CAST(:loc AS text) IS NULL OR a.location_id = :loc)
        GROUP BY a.lead_day, a.model ORDER BY a.lead_day, a.model
        """,
        {"var": variable, "window": window_days, "loc": location_id},
    )
    frame["mae"] = display_error(variable, frame["mae"])
    return frame


def best_model_by_location(
    engine: Engine, variable: str, window_days: int, lead_day: int
) -> pd.DataFrame:
    """For each location, the model with the lowest MAE (and its margin over the next)."""
    frame = _read(
        engine,
        """
        WITH bounds AS (SELECT max(valid_date) AS hi FROM gold.accuracy_daily),
        per_model AS (
            SELECT a.location_id, a.model, sum(a.n * a.mae) / sum(a.n) AS mae, sum(a.n) AS n
            FROM gold.accuracy_daily a, bounds b
            WHERE a.variable = :var AND a.lead_day = :lead AND a.valid_date > b.hi - :window
            GROUP BY a.location_id, a.model
        ),
        ranked AS (
            SELECT *, rank() OVER (PARTITION BY location_id ORDER BY mae) AS r,
                   lead(mae) OVER (PARTITION BY location_id ORDER BY mae) AS next_mae
            FROM per_model
        )
        SELECT r.location_id, d.name AS location, r.model AS best_model, r.mae,
               r.next_mae - r.mae AS margin, r.n
        FROM ranked r JOIN silver.dim_location d USING (location_id)
        WHERE r.r = 1 ORDER BY d.name
        """,
        {"var": variable, "lead": lead_day, "window": window_days},
    )
    for column in ("mae", "margin"):
        frame[column] = display_error(variable, frame[column])
    return frame


# --- briefings and LLM usage (Phase 5) ------------------------------------------------------


def latest_briefings(engine: Engine) -> pd.DataFrame:
    """The newest briefing per location, grounded or not (flagged ones are shown, marked)."""
    return _read(
        engine,
        "SELECT DISTINCT ON (b.location_id) b.briefing_id, b.location_id, d.name AS location, "
        "b.as_of, b.trigger, b.trigger_ref, b.model, b.prompt_version, b.headline, b.summary, "
        "b.confidence, b.most_reliable_model, b.reliable_reason, b.notable_risks, "
        "b.grounding_passed, b.grounding_failures, b.fact_sheet, b.created_at, b.published_at "
        "FROM gold.briefing b LEFT JOIN silver.dim_location d USING (location_id) "
        "ORDER BY b.location_id, b.as_of DESC, b.created_at DESC",
    )


def briefing_counts(engine: Engine, days: int = 7) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT count(*) AS briefings, count(*) FILTER (WHERE grounding_passed) AS grounded, "
        "count(*) FILTER (WHERE NOT grounding_passed) AS flagged, "
        "count(published_at) AS published FROM gold.briefing "
        "WHERE created_at >= now() - make_interval(days => :d)",
        {"d": days},
    )


def llm_usage_by_outcome(engine: Engine, days: int = 7) -> pd.DataFrame:
    """Calls, tokens, cost and latency per model and outcome. A cache hit or a skipped
    (disabled) request is logged too, with zero tokens, so the cache hit rate is visible."""
    return _read(
        engine,
        "SELECT model, outcome, count(*) AS calls, sum(input_tokens) AS input_tokens, "
        "sum(output_tokens) AS output_tokens, sum(cost_usd) AS cost_usd, "
        "avg(latency_ms) FILTER (WHERE outcome NOT IN ('cache_hit', 'disabled')) "
        "AS avg_latency_ms, "
        "sum(latency_ms) FILTER (WHERE outcome NOT IN ('cache_hit', 'disabled')) "
        "AS total_latency_ms FROM ops.llm_calls "
        "WHERE called_at >= now() - make_interval(days => :d) GROUP BY 1, 2 ORDER BY 1, 2",
        {"d": days},
    )


def llm_usage_daily(engine: Engine, days: int = 30) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT date_trunc('day', called_at) AS day, sum(cost_usd) AS cost_usd, "
        "count(*) FILTER (WHERE outcome NOT IN ('cache_hit', 'disabled')) AS api_calls, "
        "count(*) FILTER (WHERE outcome = 'cache_hit') AS cache_hits "
        "FROM ops.llm_calls WHERE called_at >= now() - make_interval(days => :d) "
        "GROUP BY 1 ORDER BY 1",
        {"d": days},
    )


def recent_llm_calls(engine: Engine, limit: int = 50) -> pd.DataFrame:
    return _read(
        engine,
        "SELECT called_at, purpose, model, prompt_version, attempt, outcome, input_tokens, "
        "output_tokens, latency_ms, cost_usd, left(error, 200) AS error, request_id "
        "FROM ops.llm_calls ORDER BY id DESC LIMIT :n",
        {"n": limit},
    )
