"""The whole eval against a real Postgres: every question's reference SQL, parameters, scripted
plan (through the real tools, the SQL guard and the read-only role) and grading.

The seeded verification rows are the only hand-made data; gold.accuracy_daily is computed from
them by the pipeline's own `aggregate_accuracy`, so the questions whose reference SQL reads the
raw rows while the baseline reads the daily aggregates really do cross-check the two. Errors
grow with lead time (faster at mountain locations) and GFS over-forecasts wind, so the
questions have real answers to find rather than ties."""

import random
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import Engine, text

from nimbus.agent.evals import load_questions, run_eval, run_question, save_run
from nimbus.agent.llm import ScriptedAgentClient
from nimbus.common.config import load_llm_config
from nimbus.common.db import chunked_upsert, make_engine
from nimbus.common.settings import Settings
from nimbus.common.tables import (
    accuracy_daily_table,
    forecast_verification_table,
    observation_table,
)
from nimbus.gold.verification import aggregate_accuracy
from nimbus.jobs.load_dimensions import load_dimensions

pytestmark = pytest.mark.integration

CONFIG = load_llm_config()
QUESTIONS = {q.id: q for q in load_questions()}
NOW = datetime.now(UTC).replace(microsecond=0)
ANCHOR = NOW.replace(hour=0, minute=0, second=0) - timedelta(days=1)

LOCATIONS = {  # location -> how fast its error grows per lead day
    "london": 0.15, "denver": 0.35, "kathmandu": 0.4, "san-francisco": 0.12, "reykjavik": 0.2,
    "singapore": 0.1, "tokyo": 0.15, "phoenix": 0.18, "dubai": 0.14, "cairo": 0.22,
    "chicago": 0.25, "sydney": 0.16,
}  # fmt: skip
SCALE = {"temperature_2m": 1.0, "dew_point_2m": 1.2, "wind_speed_10m": 0.8, "pressure_msl": 120.0}
BASE = {"temperature_2m": 288.0, "dew_point_2m": 280.0, "wind_speed_10m": 5.0,
        "pressure_msl": 101_300.0}  # fmt: skip
BIAS = {
    ("gfs_seamless", "wind_speed_10m"): 0.6, ("ecmwf_ifs025", "wind_speed_10m"): -0.3,
    ("ecmwf_ifs025", "temperature_2m"): 0.3, ("gfs_seamless", "temperature_2m"): -0.4,
}  # fmt: skip


def _verification_rows() -> list[dict[str, Any]]:
    rng = random.Random(6)
    rows = []
    for location, growth in LOCATIONS.items():
        for model in ("ecmwf_ifs025", "gfs_seamless", "icon_seamless"):
            for variable, scale in SCALE.items():
                for lead_day in range(1, 8):
                    for step in range(20):  # 10 days, twice a day
                        valid = ANCHOR - timedelta(hours=12 * step)
                        lead_hours = 24 * lead_day - 6
                        spread = scale * (0.6 + growth * lead_day)
                        error = BIAS.get((model, variable), 0.0) * scale + rng.gauss(0, spread)
                        observed = BASE[variable] + rng.gauss(0, scale * 3)
                        rows.append(
                            {"model": model, "location_id": location,
                             "init_time": valid - timedelta(hours=lead_hours),
                             "valid_time": valid, "variable": variable, "lead_hours": lead_hours,
                             "lead_day": lead_day, "forecast_value": observed + error,
                             "observed_value": observed, "observed_at": valid,
                             "obs_offset_seconds": 0, "error": error,
                             "ingestion_mode": "backfill"}
                        )  # fmt: skip
    return rows


def _seed(rw: Engine) -> None:
    with rw.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE gold.forecast_verification, gold.accuracy_daily, gold.alert, "
                "silver.observation, ops.ingestion_runs, ops.quality_results, "
                "ops.reconciliation_results, ops.replay_proposals, ops.agent_sessions CASCADE"
            )
        )
    load_dimensions(rw)
    verification = _verification_rows()
    chunked_upsert(
        rw, forecast_verification_table,
        ["model", "location_id", "init_time", "valid_time", "variable"],
        ["lead_hours", "lead_day", "forecast_value", "observed_value", "observed_at",
         "obs_offset_seconds", "error", "ingestion_mode"],
        verification,
    )  # fmt: skip
    frame = pd.DataFrame(verification)
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True)
    chunked_upsert(
        rw,
        accuracy_daily_table,
        ["valid_date", "location_id", "model", "variable", "lead_day"],
        ["n", "bias", "mae", "rmse"],
        aggregate_accuracy(frame).to_dict("records"),
    )

    with rw.connect() as conn:
        stations = [r[0] for r in conn.execute(text("SELECT station FROM silver.dim_location"))]

    def obs(station: str, minutes_ago: int, value: float | None, event_id: str) -> dict[str, Any]:
        return {
            "station": station,
            "observed_at": NOW - timedelta(minutes=minutes_ago),
            "variable": "temperature_2m",
            "value": value,
            "raw_text": f"METAR {station}",
            "is_corrected": False,
            "ingestion_mode": "live",
            "source_event_id": event_id,
        }

    # every station reported; the later it is alphabetically, the longer ago
    observations = [
        obs(station, 60 * (2 + i), 285.0 + i, f"obs-{i}")
        for i, station in enumerate(sorted(stations))
    ]
    observations.append(obs("EGLL", 50, 290.65, "egll-latest"))
    # a newer report without a temperature must not count as the latest temperature
    observations.append(obs("EGLL", 20, None, "egll-null"))
    chunked_upsert(
        rw, observation_table, ["station", "observed_at", "variable"],
        ["value", "raw_text", "is_corrected", "ingestion_mode", "source_event_id"], observations,
    )  # fmt: skip

    with rw.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.ingestion_runs (source, ingestion_mode, status, started_at, "
                "finished_at, messages_produced, messages_failed, error_message) VALUES "
                "('forecast_backfill', 'backfill', 'success', :t2, :t2, 400, 0, NULL), "
                "('observation_backfill', 'backfill', 'success', :t1, :t1, 700, 2, NULL), "
                "('observation_producer', 'live', 'failed', :t0, :t0, 0, 25, 'timed out')"
            ),
            {"t2": NOW - timedelta(days=2), "t1": NOW - timedelta(days=1),
             "t0": NOW - timedelta(hours=1)},
        )  # fmt: skip
        conn.execute(
            text(
                "INSERT INTO ops.quality_results (checked_at, context, table_name, check_name, "
                "severity, subject, rows_checked, rows_failed, passed) VALUES "
                "(:recent, 'silver', 'silver.observation', 'plausible_range', 'warning', NULL, "
                "100, 3, false), "
                "(:recent, 'silver', 'silver.observation', 'hard_range', 'blocking', NULL, "
                "100, 0, true), "
                "(:old, 'silver', 'silver.forecast', 'hard_range', 'blocking', NULL, "
                "100, 1, false), "
                "(:recent, 'freshness', 'silver.observation', 'freshness', 'warning', 'KDEN', "
                "1, 1, false)"
            ),
            {"recent": NOW - timedelta(hours=2), "old": NOW - timedelta(days=3)},
        )
        conn.execute(
            text(
                "INSERT INTO ops.reconciliation_results (checked_at, topic, produced_messages, "
                "bronze_messages, bronze_distinct_events, bronze_unparseable, "
                "expected_silver_rows, silver_rows, missing_from_silver, extra_in_silver, "
                "matched) VALUES "
                "(:old, 'weather.forecast.raw.v1', 10, 10, 10, 0, 100, 100, 0, 0, true), "
                "(:old, 'weather.observation.raw.v1', 10, 10, 10, 0, 40, 40, 0, 0, true), "
                "(:recent, 'weather.observation.raw.v1', 12, 12, 12, 0, 48, 47, 1, 0, false)"
            ),
            {"recent": NOW - timedelta(hours=1), "old": NOW - timedelta(days=2)},
        )
        for i, rule in enumerate(("run_change", "run_change", "run_change", "model_spread")):
            conn.execute(
                text(
                    "INSERT INTO gold.alert (alert_id, rule, severity, location_id, variable, "
                    "model, event_time, metric, threshold, details, triggered_by_event_id, "
                    "detected_at) VALUES (:id, :rule, 'warning', 'denver', 'temperature_2m', "
                    "'gfs_seamless', :t, 4.0, 3.0, '{}', :id, :t)"
                ),
                {"id": f"alert-{i}", "rule": rule, "t": NOW - timedelta(hours=i + 1)},
            )


@pytest.fixture(scope="module")
def engines(pg_settings: Settings) -> Iterator[tuple[Engine, Engine, Settings]]:
    settings = pg_settings.model_copy(update={"kafka_bootstrap_servers": "127.0.0.1:1"})
    rw, ro = make_engine(settings), make_engine(settings, readonly=True)
    _seed(rw)
    yield rw, ro, settings
    rw.dispose()
    ro.dispose()


def _run(engines: tuple[Engine, Engine, Settings], ids: list[str] | None = None) -> Any:
    rw, ro, settings = engines
    questions = [q for q in QUESTIONS.values() if ids is None or q.id in ids]
    return run_eval(questions, readonly_engine=ro, engine=rw, settings=settings, config=CONFIG)


def test_the_scripted_baseline_answers_every_question(
    engines: tuple[Engine, Engine, Settings], tmp_path: Path
) -> None:
    run = _run(engines)

    report = {r.question_id: (r.status, r.failures, r.answer) for r in run.results}
    assert all(r.status == "passed" for r in run.results), report
    summary = run.summary()
    assert summary["accuracy"] == 1.0 and summary["skipped"] == 0
    assert summary["cost_usd"] == 0.0  # the scripted client is never billed
    assert summary["tool_calls"] >= len(QUESTIONS)

    rw = engines[0]
    with rw.connect() as conn:
        sessions = conn.execute(
            text("SELECT count(*) FROM ops.agent_sessions WHERE purpose = 'eval'")
        ).scalar()
        proposals = conn.execute(text("SELECT status, decided_by FROM ops.replay_proposals")).all()
    assert sessions == len(QUESTIONS)
    # the replay question filed a proposal; the grader closed it, nothing is left pending
    assert [tuple(p) for p in proposals] == [("rejected", "make eval")]

    path = save_run(run, tmp_path)
    assert path.exists() and (tmp_path / "history.jsonl").read_text().count("\n") == 1


def test_parameters_pick_the_stalest_station(engines: tuple[Engine, Engine, Settings]) -> None:
    [result] = _run(engines, ["q07_station_without_recent_data"]).results
    with engines[0].connect() as conn:
        stalest = conn.execute(text("SELECT max(station) FROM silver.dim_location")).scalar()
    assert result.status == "passed"
    assert stalest in result.question and "status failed" in (result.answer or "")


def test_the_grader_fails_an_agent_that_picks_the_wrong_model(
    engines: tuple[Engine, Engine, Settings],
) -> None:
    rw, ro, settings = engines
    q = QUESTIONS["q01_best_model_3day_temperature_london"]
    wrong = ScriptedAgentClient(
        q.plan,
        q.answer_template.replace("rows.0.", "rows.-1."),  # the worst model instead
    )
    graded = run_question(
        q, readonly_engine=ro, engine=rw, settings=settings, config=CONFIG, client=wrong
    )
    assert graded.status == "failed"
    assert any("mentioned first" in f for f in graded.failures)


def test_an_agent_that_runs_out_of_turns_fails(engines: tuple[Engine, Engine, Settings]) -> None:
    rw, ro, settings = engines
    q = QUESTIONS["q09_verified_forecast_count_tokyo"]
    looping = ScriptedAgentClient(q.plan, q.answer_template, repeat_forever=True)
    graded = run_question(
        q, readonly_engine=ro, engine=rw, settings=settings, config=CONFIG, client=looping
    )
    assert graded.status == "failed" and graded.failures == ["no answer: max_iterations"]


def test_a_question_without_data_is_skipped_not_failed(
    engines: tuple[Engine, Engine, Settings],
) -> None:
    rw, ro, settings = engines
    q = replace(
        QUESTIONS["q12_most_frequent_alert_rule"],
        reference_sql="SELECT rule, count(*) AS alerts FROM gold.alert WHERE false GROUP BY rule",
    )
    graded = run_question(q, readonly_engine=ro, engine=rw, settings=settings, config=CONFIG)
    assert graded.status == "skipped" and graded.session_id is None  # no session was run
