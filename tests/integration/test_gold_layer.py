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
