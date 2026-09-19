"""The batch path: rows per message in plain Python, one DataFrame per batch.
(Per-message DataFrames cost ~9 ms each on the first real run - ADR 0004.)"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from nimbus.common.events import EventEnvelope
from nimbus.common.schemas import ObservationRawPayload
from nimbus.streaming import observation_silver
from nimbus.transform.observation import (
    OBSERVATION_COLUMNS,
    explode_observation_payload,
    observation_frame,
    observation_rows,
)


def _payload(i: int = 0, **report: Any) -> ObservationRawPayload:
    base = {"temp": 20.0 + i, "dewp": 10.0, "wspd": 10.0, "slp": 1015.0, "rawOb": f"KSFO {i}"}
    return ObservationRawPayload(
        station="KSFO",
        observed_at=datetime(2026, 9, 17, 12, i, tzinfo=UTC),
        api_response={**base, **report},
    )


def _message(i: int) -> bytes:
    envelope = EventEnvelope[ObservationRawPayload](
        event_id=f"e{i}",
        source="test",
        event_type="observation.raw",
        produced_at=datetime(2026, 9, 17, tzinfo=UTC),
        ingestion_mode="live",
        payload=_payload(i),
    )
    return envelope.model_dump_json().encode()


class _Msg:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return "weather.observation.raw.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def key(self) -> bytes | None:
        return b"KSFO"

    def timestamp(self) -> tuple[int, int]:
        return (1, 0)


def test_rows_are_plain_dicts_one_per_variable_with_missing_as_nan() -> None:
    rows = observation_rows(_payload(temp=None), "live")

    assert [r["variable"] for r in rows] == [
        "temperature_2m",
        "dew_point_2m",
        "wind_speed_10m",
        "pressure_msl",
    ]
    assert all(isinstance(r, dict) for r in rows)
    assert pd.isna(rows[0]["value"])  # missing temp is NaN, never dropped
    assert rows[1]["value"] == pytest.approx(283.15)


def test_a_non_numeric_value_raises_so_the_consumer_can_dead_letter_it() -> None:
    with pytest.raises(ValueError):
        observation_rows(_payload(wspd="VRB"), "live")  # speed must be numeric


def test_one_frame_for_many_messages_keeps_dtypes_and_lineage() -> None:
    rows = [
        {**row, "source_event_id": f"e{i}"}
        for i in range(3)
        for row in observation_rows(_payload(i), "live")
    ]
    frame = observation_frame(rows)

    assert len(frame) == 3 * 4
    assert frame["value"].dtype == "float64"
    assert isinstance(frame["station"].dtype, pd.CategoricalDtype)
    assert isinstance(frame["variable"].dtype, pd.CategoricalDtype)
    assert set(frame["source_event_id"]) == {"e0", "e1", "e2"}


def test_batch_frame_equals_the_per_message_frames_concatenated() -> None:
    payloads = [_payload(i) for i in range(3)]
    batch = observation_frame([r for p in payloads for r in observation_rows(p, "live")])
    one_by_one = pd.concat(
        [explode_observation_payload(p, "live") for p in payloads], ignore_index=True
    )

    pd.testing.assert_frame_equal(
        batch[OBSERVATION_COLUMNS].astype(str), one_by_one[OBSERVATION_COLUMNS].astype(str)
    )


def test_an_empty_batch_yields_an_empty_frame_with_the_expected_columns() -> None:
    frame = observation_frame([])

    assert frame.empty
    assert list(frame.columns) == OBSERVATION_COLUMNS


def test_load_messages_upserts_one_frame_and_dead_letters_only_the_poison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        observation_silver, "upsert_observation_rows", lambda engine, frame: captured.append(frame)
    )
    poisoned: list[Exception] = []

    loaded = observation_silver.load_messages(
        [_Msg(_message(0)), _Msg(b"not json"), _Msg(None), _Msg(_message(1))],
        MagicMock(),
        lambda message, error: poisoned.append(error),
    )

    assert loaded == 2
    assert len(poisoned) == 2  # invalid JSON, and a message with no value
    assert len(captured) == 1  # a single upsert for the whole batch
    assert len(captured[0]) == 2 * 4
    assert set(captured[0]["source_event_id"]) == {"e0", "e1"}


def test_load_messages_with_only_poison_upserts_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []
    monkeypatch.setattr(
        observation_silver, "upsert_observation_rows", lambda engine, frame: called.append(True)
    )

    loaded = observation_silver.load_messages([_Msg(b"garbage")], MagicMock(), lambda m, e: None)

    assert loaded == 0
    assert called == []
