"""pandera checks (brief section 8): what is blocking, what only warns, and how a
blocked row takes its message with it. Uses real transform output, not hand-built
frames, so a dtype change in a transform shows up here."""

import math
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from test_observation_silver import _message, _Msg

from nimbus.common.config import load_variables
from nimbus.common.events import EventEnvelope
from nimbus.common.schemas import ForecastRawPayload, ObservationRawPayload
from nimbus.gold.verification import verify_forecasts
from nimbus.quality.gate import gate_frame
from nimbus.quality.runner import (
    SUMMARY_CHECK,
    CheckResult,
    QualityError,
    merge_results,
    validate_frame,
)
from nimbus.quality.schemas import (
    accuracy_checks,
    forecast_checks,
    observation_checks,
    verification_checks,
)
from nimbus.streaming import forecast_silver, observation_silver
from nimbus.transform.observation import observation_frame, observation_rows

VARIABLES = load_variables()


def _observations(*, event: str = "e0", **report: Any) -> pd.DataFrame:
    base = {"temp": 20.0, "dewp": 10.0, "wspd": 10.0, "slp": 1015.0, "rawOb": "KSFO 1"}
    payload = ObservationRawPayload(
        station="KSFO",
        observed_at=datetime(2026, 9, 17, 12, tzinfo=UTC),
        api_response={**base, **report},
    )
    rows = observation_rows(payload, "live")
    for row in rows:
        row["source_event_id"] = event
    return observation_frame(rows)


def _forecasts(values: dict[str, float] | None = None, *, event: str = "f0") -> pd.DataFrame:
    values = values or {"temperature_2m": 290.0, "pressure_msl": 101300.0}
    valid = pd.Timestamp("2026-09-18T12:00", tz="UTC")
    return pd.DataFrame(
        {
            "model": pd.Categorical(["gfs_seamless"] * len(values)),
            "location_id": pd.Categorical(["sfo"] * len(values)),
            "init_time": [valid - pd.Timedelta(days=1)] * len(values),
            "valid_time": [valid] * len(values),
            "variable": pd.Categorical(list(values)),
            "value": list(values.values()),
            "lead_hours": pd.array([24] * len(values), dtype="int32"),
            "ingestion_mode": pd.Categorical(["backfill"] * len(values)),
            "source_event_id": event,
        }
    )


def _failed(frame: pd.DataFrame, checks: Any) -> dict[str, CheckResult]:
    return {r.check: r for r in validate_frame(frame, checks).failed}


# --- silver: what passes ---------------------------------------------------


def test_clean_observation_batch_passes_everything() -> None:
    validation = validate_frame(_observations(), observation_checks(VARIABLES))

    assert not validation.has_blocking_failures
    assert validation.failed == []
    assert {r.check for r in validation.results} == {SUMMARY_CHECK}


def test_clean_forecast_batch_passes_everything() -> None:
    assert validate_frame(_forecasts(), forecast_checks(VARIABLES)).failed == []


def test_a_missing_value_is_not_a_failure() -> None:
    frame = _observations(dewp=None)  # dew point not reported
    assert math.isnan(frame.loc[frame["variable"] == "dew_point_2m", "value"].iloc[0])
    assert validate_frame(frame, observation_checks(VARIABLES)).failed == []


# --- severity: hard limits block, plausible range only warns ------------------


def test_a_value_beyond_the_hard_limit_is_blocking() -> None:
    frame = _observations(temp=500.0)  # 773 K
    validation = validate_frame(frame, observation_checks(VARIABLES))

    assert validation.has_blocking_failures
    blocking = {r.check for r in validation.failed if r.severity == "blocking"}
    assert "hard_range" in blocking


def test_a_value_outside_the_plausible_range_only_warns() -> None:
    frame = _observations(temp=66.0)  # 339 K: above 60 degC, below the hard limit
    validation = validate_frame(frame, observation_checks(VARIABLES))

    assert not validation.has_blocking_failures
    failed = [(r.check, r.severity) for r in validation.failed if r.check != SUMMARY_CHECK]
    assert failed == [("plausible_range", "warning")]


def test_pressure_left_in_hpa_is_caught() -> None:
    """The bug this suite would have found on day one: METAR pressure stored in hPa
    (~1013) next to forecasts in Pa (~101300)."""
    frame = _forecasts({"pressure_msl": 1013.0})
    assert validate_frame(frame, forecast_checks(VARIABLES)).has_blocking_failures


def test_a_forecast_valid_before_its_init_time_only_warns() -> None:
    """Found by CI: the fixture-based integration tests pair a recent run with fixed
    valid times. A negative lead is odd but harmless (gold never scores it), so it is
    flagged, not quarantined."""
    frame = _forecasts()
    frame["lead_hours"] = pd.array([-48, -48], dtype="int32")
    validation = validate_frame(frame, forecast_checks(VARIABLES))

    assert not validation.has_blocking_failures
    assert "lead_hours:lead_not_negative" in {r.check for r in validation.failed}


def test_infinite_value_is_blocking_but_nan_is_not() -> None:
    frame = _forecasts({"temperature_2m": math.inf, "pressure_msl": math.nan})
    failed = _failed(frame, forecast_checks(VARIABLES))
    assert "value:finite" in failed
    assert failed["value:finite"].rows_failed == 1


def test_unknown_variable_is_blocking() -> None:
    frame = _forecasts({"snow_depth": 1.0})
    assert "known_variable" in _failed(frame, forecast_checks(VARIABLES))


def test_naive_timestamps_are_blocking() -> None:
    frame = _forecasts()
    frame["valid_time"] = frame["valid_time"].dt.tz_localize(None)
    assert "valid_time:timezone_aware" in _failed(frame, forecast_checks(VARIABLES))


def test_null_key_is_blocking() -> None:
    frame = _forecasts()
    frame["location_id"] = pd.Categorical([None, "sfo"])
    assert validate_frame(frame, forecast_checks(VARIABLES)).has_blocking_failures


def test_duplicate_natural_keys_block_only_when_uniqueness_is_asserted() -> None:
    frame = pd.concat([_forecasts(), _forecasts(event="f1")], ignore_index=True)

    assert not validate_frame(frame, forecast_checks(VARIABLES)).has_blocking_failures
    assert validate_frame(frame, forecast_checks(VARIABLES, unique=True)).has_blocking_failures


# --- the gate: a bad row takes its whole message ------------------------------


def test_gate_drops_the_offending_message_and_keeps_the_rest() -> None:
    good = _observations(event="good")
    bad = _observations(event="bad", temp=500.0)
    gate = gate_frame(pd.concat([good, bad], ignore_index=True), observation_checks(VARIABLES))

    assert gate.blocked_events == {"bad"}
    assert set(gate.clean["source_event_id"]) == {"good"}
    assert len(gate.clean) == 4  # every row of the good message survives


def test_gate_keeps_warning_rows() -> None:
    frame = _observations(temp=66.0)
    gate = gate_frame(frame, observation_checks(VARIABLES))

    assert gate.blocked_events == frozenset()
    assert len(gate.clean) == len(frame)
    assert {(r.check, r.severity) for r in gate.failed} == {
        ("plausible_range", "warning"),
        (SUMMARY_CHECK, "warning"),
    }


def test_gate_blocks_everything_when_a_column_is_missing() -> None:
    frame = _observations().drop(columns=["raw_text"])
    gate = gate_frame(frame, observation_checks(VARIABLES))

    assert gate.blocked_events == {"e0"}
    assert gate.clean.empty


def test_consumer_sends_a_quality_failure_to_the_dlq_and_loads_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        observation_silver, "upsert_observation_rows", lambda e, f: captured.append(f)
    )
    persisted: list[Any] = []
    monkeypatch.setattr(observation_silver, "persist_results", lambda *a, **k: persisted.append(a))
    poisoned: list[Exception] = []

    # message 1 carries an impossible temperature
    bad = _message(1).replace(b'"temp":21.0', b'"temp":900.0')
    assert bad != _message(1)

    loaded = observation_silver.load_messages(
        [_Msg(_message(0)), _Msg(bad)], MagicMock(), lambda m, e: poisoned.append(e)
    )

    assert loaded == 1
    assert len(poisoned) == 1 and isinstance(poisoned[0], QualityError)
    assert set(captured[0]["source_event_id"]) == {"e0"}
    assert persisted, "the failed check was recorded in ops.quality_results"


# --- gold ---------------------------------------------------------------------


def _verification() -> pd.DataFrame:
    valid = pd.Timestamp("2026-09-18T12:00", tz="UTC")
    forecasts = _forecasts().assign(valid_time=valid)
    observations = pd.DataFrame(
        {
            "station": ["KSFO", "KSFO"],
            "observed_at": [valid, valid],
            "variable": ["temperature_2m", "pressure_msl"],
            "value": [288.0, 101000.0],
        }
    )
    return verify_forecasts(
        forecasts,
        observations,
        pd.DataFrame({"location_id": ["sfo"], "station": ["KSFO"]}),
        tolerance_minutes=30,
        min_lead_hours=1,
    )


def _gold_checks() -> Any:
    return verification_checks(VARIABLES, tolerance_minutes=30)


def test_real_verification_output_passes_the_gold_checks() -> None:
    frame = _verification()
    assert len(frame) == 2
    assert validate_frame(frame, _gold_checks()).failed == []


def test_an_error_that_is_not_forecast_minus_observed_is_blocking() -> None:
    frame = _verification()
    frame["error"] = frame["error"] + 1.0
    assert "error_is_forecast_minus_observed" in _failed(frame, _gold_checks())


def test_a_match_beyond_the_tolerance_is_blocking() -> None:
    frame = _verification()
    frame["obs_offset_seconds"] = 31 * 60
    assert "obs_offset_seconds:within_match_tolerance" in _failed(frame, _gold_checks())


def test_duplicate_verification_keys_are_blocking() -> None:
    frame = pd.concat([_verification(), _verification()], ignore_index=True)
    assert validate_frame(frame, _gold_checks()).has_blocking_failures


def test_accuracy_statistics_must_be_mutually_consistent() -> None:
    good = pd.DataFrame(
        {
            "valid_date": [datetime(2026, 9, 18).date()],
            "location_id": ["sfo"],
            "model": ["gfs_seamless"],
            "variable": ["temperature_2m"],
            "lead_day": [1],
            "n": [4],
            "bias": [0.5],
            "mae": [1.0],
            "rmse": [1.2],
        }
    )
    assert validate_frame(good, accuracy_checks()).failed == []

    impossible = good.assign(rmse=0.5)  # RMSE can never be below MAE
    assert "rmse_ge_mae_ge_abs_bias" in _failed(impossible, accuracy_checks())


# --- results ------------------------------------------------------------------


def test_merge_results_adds_rows_across_chunks() -> None:
    a = CheckResult("t", "c", "warning", 100, 2, detail="first")
    b = CheckResult("t", "c", "warning", 50, 1, detail="second")
    other = CheckResult("t", "d", "blocking", 10, 0)

    merged = {r.check: r for r in merge_results([a, b, other])}

    assert (merged["c"].rows_checked, merged["c"].rows_failed) == (150, 3)
    assert merged["c"].detail == "first"
    assert merged["d"].passed


def test_a_duplicated_quarantined_event_sends_every_copy_to_the_dlq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Event ids are deterministic, so the same report can be in one batch twice."""
    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        observation_silver, "upsert_observation_rows", lambda e, f: captured.append(f)
    )
    monkeypatch.setattr(observation_silver, "persist_results", lambda *a, **k: None)
    bad = _message(1).replace(b'"temp":21.0', b'"temp":900.0')
    poisoned: list[Exception] = []

    loaded = observation_silver.load_messages(
        [_Msg(_message(0)), _Msg(bad), _Msg(bad)], MagicMock(), lambda m, e: poisoned.append(e)
    )

    assert loaded == 1
    assert len(poisoned) == 2  # neither copy is silently lost
    assert "hard_range" in str(poisoned[0])  # the DLQ record names the failed check
    assert set(captured[0]["source_event_id"]) == {"e0"}


def test_a_forecast_answer_with_no_hours_is_not_poison(monkeypatch: pytest.MonkeyPatch) -> None:
    upserts: list[pd.DataFrame] = []
    monkeypatch.setattr(forecast_silver, "upsert_forecast_rows", lambda e, f: upserts.append(f))
    envelope = EventEnvelope[ForecastRawPayload](
        event_id="empty",
        source="forecast_producer",
        event_type="forecast.raw",
        produced_at=datetime(2026, 9, 17, tzinfo=UTC),
        ingestion_mode="live",
        payload=ForecastRawPayload(
            model="gfs_seamless",
            location_id="sfo",
            run=datetime(2026, 9, 17, 12, tzinfo=UTC),
            api_response={"hourly": {"time": [], "temperature_2m": []}},
        ),
    )
    poisoned: list[Exception] = []

    loaded = forecast_silver.load_messages(
        [_Msg(envelope.model_dump_json().encode())], MagicMock(), lambda m, e: poisoned.append(e)
    )

    assert (loaded, poisoned, upserts) == (1, [], [])
