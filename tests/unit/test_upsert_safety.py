"""Regression tests for two failure modes that only appear at real batch sizes:
duplicate keys inside one upsert statement, and a producer outrunning the broker."""

from typing import Any

import pandas as pd
import pytest

from nimbus.common.db import dedupe_on_key
from nimbus.common.kafka import produce_json
from nimbus.streaming import observation_silver


def test_dedupe_on_key_keeps_the_last_row_per_key() -> None:
    rows = pd.DataFrame(
        {"k": ["a", "a", "b"], "v": [1, 2, 3]},
    )
    result = dedupe_on_key(rows, ["k"])

    assert dict(zip(result["k"], result["v"], strict=True)) == {"a": 2, "b": 3}


def test_a_correction_outranks_the_original_when_both_are_in_one_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_chunked_upsert(*args: Any, **kwargs: Any) -> None:
        captured["records"] = args[4]

    monkeypatch.setattr(observation_silver, "chunked_upsert", fake_chunked_upsert)

    observed_at = pd.Timestamp("2026-09-17T23:53:00", tz="UTC")
    common = {
        "station": "KDEN",
        "observed_at": observed_at,
        "variable": "temperature_2m",
        "ingestion_mode": "live",
    }
    # The correction arrived FIRST in the batch on purpose - is_corrected, not
    # arrival order, must decide the winner.
    rows = pd.DataFrame(
        [
            {
                **common,
                "value": 300.0,
                "raw_text": "METAR KDEN COR",
                "is_corrected": True,
                "source_event_id": "cor",
            },
            {
                **common,
                "value": 999.0,
                "raw_text": "METAR KDEN",
                "is_corrected": False,
                "source_event_id": "orig",
            },
        ]
    )
    observation_silver.upsert_observation_rows(engine=None, rows=rows)  # type: ignore[arg-type]

    records = captured["records"]
    assert len(records) == 1
    assert records[0]["source_event_id"] == "cor"
    assert records[0]["is_corrected"] is True


class _FlakyProducer:
    """Raises BufferError a couple of times (queue full), then accepts."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.produced: list[bytes] = []
        self.polls: list[float] = []

    def produce(self, **kwargs: Any) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise BufferError("Local: Queue full")
        self.produced.append(kwargs["value"])

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        return 0


def test_produce_json_waits_and_retries_when_the_queue_is_full() -> None:
    producer = _FlakyProducer(failures=2)
    produce_json(producer, "t", "k", {"a": 1})  # type: ignore[arg-type]

    assert len(producer.produced) == 1  # delivered exactly once, not dropped or duplicated
    assert producer.polls.count(1.0) == 2  # served callbacks once per BufferError
