"""Phase 3 acceptance for the gold layer, against a real Postgres: results are
correct and reproducible, re-running changes nothing, an incremental run
recomputes only the days that saw new data, and the leaderboard ranks models.
No Kafka needed - silver is populated through the same upsert functions the
consumers use, so change tracking (`updated_at`) is exercised for real."""

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import Engine, text

from nimbus.common.config import GoldConfig, load_locations
from nimbus.common.db import chunked_upsert, make_engine
from nimbus.common.settings import Settings
from nimbus.common.tables import accuracy_daily_table
from nimbus.gold.build import build_gold
from nimbus.jobs.load_dimensions import load_dimensions
from nimbus.streaming.forecast_silver import upsert_forecast_rows
from nimbus.streaming.observation_silver import upsert_observation_rows

LOCATION = load_locations()[0]
VAR = "temperature_2m"
# lookback 0: silver rows inserted seconds before the first build must not look
# "recent" to the second one, or every day would be recomputed.
CONFIG = GoldConfig(observation_match_tolerance_minutes=30, min_lead_hours=1, lookback_hours=0)
FIRST_DAY = date(2026, 1, 2)

pytestmark = pytest.mark.integration


def _ts(day: date, hour: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


@pytest.fixture
def engine(pg_settings: Settings) -> Iterator[Engine]:
    eng = make_engine(pg_settings)
    _clean(eng)
    load_dimensions(eng)
    yield eng
    _clean(eng)
    eng.dispose()


def _clean(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE silver.forecast, silver.observation, gold.forecast_verification, "
                "gold.accuracy_daily, ops.gold_build_log, ops.job_state"
            )
        )


def _forecast_rows(
    day: date,
    *,
    error: float,
    lead_days: int = 1,
    model: str = "gfs_seamless",
    truth: float = 280.0,
) -> pd.DataFrame:
    """Hourly forecasts valid on `day`, each `error` away from the observation."""
    valid = [_ts(day, h) for h in range(0, 24, 6)]
    return pd.DataFrame(
        {
            "model": model,
            "location_id": LOCATION.id,
            "init_time": [t - timedelta(days=lead_days) for t in valid],
            "valid_time": valid,
            "variable": VAR,
            "value": truth + error,
            "lead_hours": lead_days * 24,
            "ingestion_mode": "backfill",
            "source_event_id": f"fc-{model}-{day}-{lead_days}",
        }
    )


def _observation_rows(
    day: date, *, value: float = 280.0, is_corrected: bool = False, event: str = "obs-1"
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": LOCATION.station,
            "observed_at": [_ts(day, h) for h in range(0, 24, 6)],
            "variable": VAR,
            "value": value,
            "raw_text": "METAR",
            "is_corrected": is_corrected,
            "ingestion_mode": "backfill",
            "source_event_id": f"{event}-{day}",
        }
    )


def _table(engine: Engine, name: str, order: str) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        return [
            dict(r) for r in conn.execute(text(f"SELECT * FROM {name} ORDER BY {order}")).mappings()
        ]


def _verification(engine: Engine) -> list[dict[str, Any]]:
    return _table(
        engine,
        "gold.forecast_verification",
        "valid_time, model, lead_hours, variable",
    )


def _accuracy(engine: Engine) -> list[dict[str, Any]]:
    return _table(engine, "gold.accuracy_daily", "valid_date, model, lead_day")


def _seed(engine: Engine, days: int = 2) -> None:
    for i in range(days):
        day = FIRST_DAY + timedelta(days=i)
        upsert_observation_rows(engine, _observation_rows(day))
        upsert_forecast_rows(engine, _forecast_rows(day, error=1.0, lead_days=1))
        upsert_forecast_rows(engine, _forecast_rows(day, error=3.0, lead_days=3))


def test_verification_and_accuracy_are_correct_and_error_rises_with_lead(engine: Engine) -> None:
    _seed(engine)
    result = build_gold(engine, config=CONFIG)

    assert result.run_kind == "full"
    assert [d.valid_date for d in result.days] == [FIRST_DAY, FIRST_DAY + timedelta(days=1)]

    rows = _verification(engine)
    assert len(rows) == 2 * 4 * 2  # days x 6-hourly steps x lead days
    assert {r["obs_offset_seconds"] for r in rows} == {0}
    lead1 = next(r for r in rows if r["lead_day"] == 1)
    assert lead1["error"] == pytest.approx(1.0)
    assert lead1["forecast_value"] == pytest.approx(281.0)
    assert lead1["observed_value"] == pytest.approx(280.0)

    daily = _accuracy(engine)
    assert len(daily) == 4
    by_lead = {(r["valid_date"], r["lead_day"]): r for r in daily}
    d1 = by_lead[(FIRST_DAY, 1)]
    d3 = by_lead[(FIRST_DAY, 3)]
    assert (d1["n"], d1["bias"], d1["mae"], d1["rmse"]) == (
        4,
        pytest.approx(1.0),
        pytest.approx(1.0),
        pytest.approx(1.0),
    )
    assert d3["mae"] > d1["mae"] and d3["rmse"] > d1["rmse"]


def test_rerunning_changes_nothing(engine: Engine) -> None:
    _seed(engine)
    build_gold(engine, config=CONFIG)
    verification = _verification(engine)
    accuracy = _accuracy(engine)

    # Incremental with nothing new, then a forced full recompute, then an
    # incremental run whose lookback re-reads every recent day: all leave
    # every row - including computed_at - untouched.
    assert build_gold(engine, config=CONFIG).days == []
    build_gold(engine, full=True, config=CONFIG)
    build_gold(
        engine,
        config=GoldConfig(
            observation_match_tolerance_minutes=30, min_lead_hours=1, lookback_hours=24
        ),
    )
    assert _verification(engine) == verification
    assert _accuracy(engine) == accuracy


def test_incremental_recomputes_only_days_touched_by_new_data(engine: Engine) -> None:
    _seed(engine, days=5)
    build_gold(engine, config=CONFIG)
    before = _accuracy(engine)

    touched = FIRST_DAY + timedelta(days=3)
    upsert_observation_rows(engine, _observation_rows(touched, value=279.0, event="obs-2"))
    result = build_gold(engine, config=CONFIG)

    assert result.run_kind == "incremental"
    # the changed day plus its neighbours (a match can straddle midnight) - not the rest
    assert [d.valid_date for d in result.days] == [
        touched - timedelta(days=1),
        touched,
        touched + timedelta(days=1),
    ]
    after = {(r["valid_date"], r["model"], r["lead_day"]): r for r in _accuracy(engine)}
    for row in before:
        new = after[(row["valid_date"], row["model"], row["lead_day"])]
        if row["valid_date"] == touched:
            assert new["bias"] == pytest.approx(row["bias"] + 1.0)  # obs 1 K colder -> error +1
        else:
            assert new == row  # untouched days are byte-identical, computed_at included


def test_forecasts_wait_for_observations_then_verify_when_they_arrive(engine: Engine) -> None:
    upsert_forecast_rows(engine, _forecast_rows(FIRST_DAY, error=2.0))
    assert build_gold(engine, config=CONFIG).days == []
    assert _verification(engine) == []

    upsert_observation_rows(engine, _observation_rows(FIRST_DAY))
    result = build_gold(engine, config=CONFIG)
    assert FIRST_DAY in [d.valid_date for d in result.days]
    assert len(_verification(engine)) == 4


def test_rows_that_stop_verifying_are_removed(engine: Engine) -> None:
    _seed(engine, days=1)
    build_gold(engine, config=CONFIG)
    assert len(_verification(engine)) == 8

    # A correction that blanks the value: the observation is no longer usable.
    upsert_observation_rows(
        engine, _observation_rows(FIRST_DAY, value=float("nan"), is_corrected=True, event="cor")
    )
    build_gold(engine, config=CONFIG)
    assert _verification(engine) == []
    assert _accuracy(engine) == []


def test_silver_upsert_only_bumps_updated_at_when_a_row_changes(engine: Engine) -> None:
    frame = _forecast_rows(FIRST_DAY, error=1.0)

    def stamps() -> list[Any]:
        with engine.connect() as conn:
            return list(
                conn.execute(
                    text("SELECT updated_at FROM silver.forecast ORDER BY valid_time")
                ).scalars()
            )

    upsert_forecast_rows(engine, frame)
    first = stamps()
    upsert_forecast_rows(engine, frame)  # redelivery of the same event
    assert stamps() == first

    upsert_forecast_rows(engine, frame.assign(value=frame["value"] + 0.5))  # revised
    assert all(new > old for new, old in zip(stamps(), first, strict=True))


def test_leaderboard_ranks_models_by_window(engine: Engine) -> None:
    for i in range(3):
        day = FIRST_DAY + timedelta(days=i)
        upsert_observation_rows(engine, _observation_rows(day))
        upsert_forecast_rows(engine, _forecast_rows(day, error=1.0, model="gfs_seamless"))
        upsert_forecast_rows(engine, _forecast_rows(day, error=3.0, model="icon_seamless"))
    build_gold(engine, config=CONFIG)

    # An old day inside the 30-day window but outside the 7-day one, where the
    # worse model happened to be better - it must not affect the 7-day ranking.
    old = FIRST_DAY - timedelta(days=15)
    stale_rows = [
        {
            "valid_date": old,
            "location_id": LOCATION.id,
            "model": model,
            "variable": VAR,
            "lead_day": 1,
            "n": 4,
            "bias": 0.0,
            "mae": mae,
            "rmse": mae,
        }
        for model, mae in (("gfs_seamless", 5.0), ("icon_seamless", 0.5))
    ]
    chunked_upsert(
        engine,
        accuracy_daily_table,
        ["valid_date", "location_id", "model", "variable", "lead_day"],
        ["n", "bias", "mae", "rmse"],
        stale_rows,
    )

    with engine.connect() as conn:
        board = {
            (r.window_days, r.model): r
            for r in conn.execute(text("SELECT * FROM gold.model_leaderboard"))
        }
    assert board[(7, "gfs_seamless")].mae_rank == 1
    assert board[(7, "icon_seamless")].mae_rank == 2
    assert board[(7, "gfs_seamless")].n == 12
    assert board[(7, "gfs_seamless")].mae == pytest.approx(1.0)
    assert board[(7, "icon_seamless")].rmse == pytest.approx(3.0)
    # 30-day window: (12*1 + 4*5) / 16 = 2.0 for gfs; (12*3 + 4*0.5) / 16 = 2.375 for icon
    assert board[(30, "gfs_seamless")].n == 16
    assert board[(30, "gfs_seamless")].mae == pytest.approx(2.0)
    assert board[(30, "icon_seamless")].mae == pytest.approx(2.375)
    assert board[(30, "gfs_seamless")].mae_rank == 1


# --- quality (Phase 3b) ---------------------------------------------------------


def _quality_rows(engine: Engine, context_like: str) -> list[dict[str, Any]]:
    return _table(
        engine,
        f"ops.quality_results WHERE context LIKE '{context_like}'",
        "id",
    )


def test_a_gold_build_records_its_quality_results(engine: Engine) -> None:
    _seed(engine, days=1)
    build_gold(engine, config=CONFIG)

    rows = _quality_rows(engine, "gold-build%")
    tables = {(r["table_name"], r["severity"]) for r in rows}
    assert ("gold.forecast_verification", "blocking") in tables
    assert ("gold.accuracy_daily", "blocking") in tables
    assert all(r["passed"] for r in rows)


def test_a_blocking_failure_aborts_the_build_and_leaves_gold_untouched(engine: Engine) -> None:
    from nimbus.quality.runner import QualityError

    _seed(engine, days=2)
    build_gold(engine, config=CONFIG)
    before = _verification(engine)
    watermark_before = _table(engine, "ops.job_state", "job")

    # An impossible observation (773 K) that reached silver by some route that
    # bypassed the consumer gate - gold must still refuse to score against it.
    upsert_observation_rows(engine, _observation_rows(FIRST_DAY, value=773.0, event="bad"))
    with pytest.raises(QualityError, match="hard_range"):
        build_gold(engine, config=CONFIG)

    assert _verification(engine) == before
    assert _table(engine, "ops.job_state", "job") == watermark_before  # will be retried
    failed = [r for r in _quality_rows(engine, "gold-build%") if not r["passed"]]
    assert any(r["check_name"] == "hard_range_o" and r["severity"] == "blocking" for r in failed)


def test_the_quality_suite_validates_silver_and_gold_and_records_everything(
    engine: Engine,
) -> None:
    from nimbus.jobs.run_quality import run_suite
    from nimbus.quality.runner import persist_results

    _seed(engine)
    build_gold(engine, config=CONFIG)

    results = run_suite(engine, since=None)
    persist_results(engine, results, context="quality-suite")

    tables = {r.table for r in results}
    assert {"silver.forecast", "silver.observation", "gold.forecast_verification"} <= tables
    assert not [r for r in results if r.severity == "blocking" and not r.passed]
    assert len(_quality_rows(engine, "quality-suite")) == len(results)


def test_freshness_flags_quiet_stations_and_sources(engine: Engine) -> None:
    from nimbus.common.config import QualityConfig
    from nimbus.quality.freshness import check_freshness

    now = datetime(2026, 1, 2, 12, tzinfo=UTC)
    fresh = pd.DataFrame(
        {
            "station": LOCATION.station,
            "observed_at": [now - timedelta(hours=1)],
            "variable": VAR,
            "value": 280.0,
            "raw_text": "METAR",
            "is_corrected": False,
            "ingestion_mode": "live",
            "source_event_id": "fresh",
        }
    )
    upsert_observation_rows(engine, fresh)
    config = QualityConfig(observation_max_age_hours=3, forecast_max_run_age_hours=14)

    results = {(r.table, r.subject): r for r in check_freshness(engine, config, now=now)}

    assert results[("silver.observation", LOCATION.station)].passed
    other = next(
        r for (t, s), r in results.items() if t == "silver.observation" and s != LOCATION.station
    )
    assert not other.passed and other.detail == "no data yet"
    assert not results[("ops.ingestion_runs", "forecast_producer")].passed  # never ran

    later = check_freshness(engine, config, now=now + timedelta(hours=5))
    stale = next(r for r in later if r.subject == LOCATION.station)
    assert not stale.passed and "6.0 h old" in (stale.detail or "")


# --- lineage (Phase 3c) ---------------------------------------------------------


def test_trace_follows_an_observation_from_bronze_to_the_gold_metric_it_fed(
    engine: Engine, tmp_path: Any
) -> None:
    import json

    from nimbus.jobs.trace import format_trace, trace_event
    from nimbus.streaming.bronze_reader import BronzeMessage
    from nimbus.streaming.bronze_sink import write_batch_to_parquet
    from nimbus.streaming.observation_silver import message_to_rows
    from nimbus.transform.observation import observation_frame

    observed_at = _ts(FIRST_DAY, 12)

    def envelope(event_id: str, temp: float) -> bytes:
        return json.dumps(
            {
                "event_id": event_id,
                "schema_version": 1,
                "source": "observation_producer",
                "event_type": "observation.raw",
                "produced_at": observed_at.isoformat(),
                "ingestion_mode": "live",
                "payload": {
                    "station": LOCATION.station,
                    "observed_at": observed_at.isoformat(),
                    "api_response": {
                        "temp": temp,
                        "dewp": 5.0,
                        "wspd": 5,
                        "slp": 1013.0,
                        "rawOb": "METAR",
                    },
                },
            }
        ).encode()

    def land(event_id: str, temp: float, offset: int) -> None:
        raw = envelope(event_id, temp)
        write_batch_to_parquet(
            [BronzeMessage("weather.observation.raw.v1", 3, offset, 1_700_000_000_000, b"k", raw)],
            "weather.observation.raw.v1",
            tmp_path,
        )
        upsert_observation_rows(engine, observation_frame(message_to_rows(raw)))

    land("evt-original", 20.0, offset=41)  # 293.15 K
    forecast = _forecast_rows(FIRST_DAY, error=2.0, truth=293.15).iloc[[2]]  # valid 12:00
    upsert_forecast_rows(engine, forecast)
    build_gold(engine, config=CONFIG)

    trace = trace_event(engine, "evt-original", tmp_path)

    assert (trace.bronze[0].partition, trace.bronze[0].offset) == (3, 41)
    assert trace.ingestion_run is None  # no producer run was recorded in this test
    assert (trace.silver_expected, trace.silver_current) == (4, 4)
    assert trace.gold_verification == 1  # only temperature had a forecast to score
    assert len(trace.gold_accuracy) == 1
    assert trace.gold_accuracy[0]["bias"] == pytest.approx(2.0)
    text_report = format_trace(trace)
    assert "partition 3" in text_report and "offset 41" in text_report

    # A later event overwrites the rows: the trace says so instead of losing the event.
    land("evt-revised", 21.0, offset=42)
    later = trace_event(engine, "evt-original", tmp_path)
    assert later.silver_current == 0
    assert later.silver_by_event == {"evt-revised": 4}
    assert "overwritten by event evt-revised" in format_trace(later)


# --- review follow-ups ------------------------------------------------------------


def test_gold_survives_the_silver_forecasts_it_was_built_from_being_dropped(
    engine: Engine,
) -> None:
    """Retention drops old silver forecast partitions; a late observation revision then
    dirties those days. The rebuild must not delete the metrics they produced."""
    _seed(engine, days=2)
    build_gold(engine, config=CONFIG)
    accuracy = _accuracy(engine)
    assert accuracy

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.forecast"))  # as if the months had been dropped
    upsert_observation_rows(engine, _observation_rows(FIRST_DAY, value=279.0, event="late"))
    result = build_gold(engine, config=CONFIG)

    assert FIRST_DAY in [d.valid_date for d in result.days]  # it was rebuilt...
    assert _accuracy(engine) == accuracy  # ...without erasing the history


def test_changing_the_matching_rules_triggers_a_full_rebuild(engine: Engine) -> None:
    _seed(engine, days=3)
    build_gold(engine, config=CONFIG)
    assert build_gold(engine, config=CONFIG).days == []  # same rules: nothing to do

    stricter = GoldConfig(
        observation_match_tolerance_minutes=10, min_lead_hours=1, lookback_hours=0
    )
    result = build_gold(engine, config=stricter)

    assert result.run_kind == "full"  # no watermark for these rules, so every day is redone
    assert len(result.days) == 3
