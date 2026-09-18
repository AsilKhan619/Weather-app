from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from confluent_kafka import TopicPartition

from nimbus.jobs import replay
from nimbus.jobs.reconcile import ReconciliationResult, diff_key_sets, key_hashes
from nimbus.streaming.bronze_reader import iter_bronze_batches, list_bronze_files
from nimbus.streaming.bronze_sink import write_batch_to_parquet

TOPIC = "weather.forecast.raw.v1"


class _Msg:
    def __init__(self, partition: int, offset: int, value: bytes) -> None:
        self._p, self._o, self._v = partition, offset, value

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_700_000_000_000 + self._o)

    def key(self) -> bytes:
        return b"k"

    def value(self) -> bytes:
        return self._v

    def topic(self) -> str:
        return TOPIC


# --- bronze reader ---------------------------------------------------------


def test_bronze_round_trip_preserves_values_and_lineage(tmp_path: Path) -> None:
    write_batch_to_parquet([_Msg(1, 7, b"a"), _Msg(0, 3, b"b")], TOPIC, lake_root=tmp_path)

    (batch,) = list(iter_bronze_batches(TOPIC, tmp_path))

    # within a file, (partition, offset) order - the order the sink received them
    assert [(m.partition(), m.offset(), m.value()) for m in batch] == [
        (0, 3, b"b"),
        (1, 7, b"a"),
    ]
    assert batch[0].topic() == TOPIC


def test_bronze_files_replay_in_write_order(tmp_path: Path) -> None:
    first = write_batch_to_parquet([_Msg(0, 1, b"first")], TOPIC, lake_root=tmp_path)
    second = write_batch_to_parquet([_Msg(0, 2, b"second")], TOPIC, lake_root=tmp_path)

    assert list_bronze_files(TOPIC, tmp_path) == [first, second]
    values = [m.value() for batch in iter_bronze_batches(TOPIC, tmp_path) for m in batch]
    assert values == [b"first", b"second"]  # a later correction must land after its original


def test_bronze_batches_are_bounded_and_a_missing_topic_is_empty(tmp_path: Path) -> None:
    write_batch_to_parquet([_Msg(0, i, b"x") for i in range(5)], TOPIC, lake_root=tmp_path)

    sizes = [len(b) for b in iter_bronze_batches(TOPIC, tmp_path, batch_size=2)]

    assert sizes == [2, 2, 1]
    assert list(iter_bronze_batches("weather.nothing.v1", tmp_path)) == []


# --- replay from bronze ----------------------------------------------------


def test_replay_loads_every_batch_and_counts_poison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_batch_to_parquet([_Msg(0, i, b"x") for i in range(3)], TOPIC, lake_root=tmp_path)

    def fake_load(messages: Sequence[Any], engine: Any, on_poison: Any) -> int:
        on_poison(messages[0], ValueError("bad"))  # first message of each batch is poison
        return len(messages) - 1

    monkeypatch.setitem(replay.TARGETS, TOPIC, replay.ReplayTarget(fake_load, "silver.forecast"))

    result = replay.replay_from_bronze(TOPIC, MagicMock(), tmp_path, batch_size=2)

    assert result.messages == 3
    assert result.poison == 2  # two batches
    assert result.loaded == 1


def test_replay_only_truncates_when_asked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(
        replay.TARGETS, TOPIC, replay.ReplayTarget(lambda m, e, p: 0, "silver.forecast")
    )

    engine = MagicMock()
    replay.replay_from_bronze(TOPIC, engine, tmp_path)
    engine.begin.assert_not_called()

    monkeypatch.setattr(replay, "unexplained_silver_rows", lambda topic, engine, lake: 0)
    engine = MagicMock()
    replay.replay_from_bronze(TOPIC, engine, tmp_path, truncate=True)
    executed = str(engine.begin.return_value.__enter__.return_value.execute.call_args[0][0])
    assert executed == "TRUNCATE TABLE silver.forecast"


def test_a_truncating_rebuild_refuses_when_silver_holds_rows_the_lake_cannot_explain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        replay.TARGETS, TOPIC, replay.ReplayTarget(lambda m, e, p: 0, "silver.forecast")
    )
    monkeypatch.setattr(replay, "unexplained_silver_rows", lambda topic, engine, lake: 7)
    engine = MagicMock()

    with pytest.raises(replay.UnsafeRebuildError, match="7 rows"):
        replay.replay_from_bronze(TOPIC, engine, tmp_path, truncate=True)

    engine.begin.assert_not_called()  # refused BEFORE deleting anything


def test_force_accepts_the_loss_and_a_non_truncating_replay_never_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        replay.TARGETS, TOPIC, replay.ReplayTarget(lambda m, e, p: 0, "silver.forecast")
    )
    checked: list[str] = []

    def guard(topic: str, engine: Any, lake: Path) -> int:
        checked.append(topic)
        return 7

    monkeypatch.setattr(replay, "unexplained_silver_rows", guard)

    replay.replay_from_bronze(TOPIC, MagicMock(), tmp_path, truncate=True, force=True)
    replay.replay_from_bronze(TOPIC, MagicMock(), tmp_path)  # no truncate: nothing at risk

    assert checked == []  # skipped both times


# --- offset reset targets --------------------------------------------------


class _FakeConsumer:
    def __init__(
        self, watermarks: dict[int, tuple[int, int]], time_offsets: dict[int, int]
    ) -> None:
        self.watermarks = watermarks
        self.time_offsets = time_offsets

    def list_topics(self, topic: str, timeout: float) -> Any:
        meta = MagicMock()
        meta.topics = {topic: MagicMock(partitions=dict.fromkeys(self.watermarks))}
        return meta

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float) -> tuple[int, int]:
        return self.watermarks[tp.partition]

    def offsets_for_times(
        self, query: list[TopicPartition], timeout: float
    ) -> list[TopicPartition]:
        return [
            TopicPartition(TOPIC, tp.partition, self.time_offsets[tp.partition]) for tp in query
        ]


def _targets(consumer: _FakeConsumer, from_time: datetime | None) -> dict[int, int]:
    arg: Any = consumer
    return {tp.partition: tp.offset for tp in replay.resolve_offset_targets(arg, TOPIC, from_time)}


def test_offset_reset_defaults_to_the_earliest_retained_offset() -> None:
    consumer = _FakeConsumer({0: (5, 50), 1: (0, 20)}, {})
    assert _targets(consumer, None) == {0: 5, 1: 0}


def test_offset_reset_by_time_uses_the_first_offset_at_or_after_it() -> None:
    consumer = _FakeConsumer({0: (0, 50), 1: (0, 20)}, {0: 12, 1: 7})
    assert _targets(consumer, datetime(2026, 9, 1, tzinfo=UTC)) == {0: 12, 1: 7}


def test_offset_reset_by_time_with_no_later_message_resolves_to_the_end() -> None:
    # offsets_for_times returns -1 (OFFSET_END) when nothing is at/after the time
    consumer = _FakeConsumer({0: (0, 50), 1: (0, 20)}, {0: -1, 1: 7})
    assert _targets(consumer, datetime(2026, 9, 1, tzinfo=UTC)) == {0: 50, 1: 7}


def test_a_naive_from_time_is_utc_not_the_machines_local_timezone() -> None:
    naive = replay.parse_utc("2026-09-15T00:00:00")
    explicit = replay.parse_utc("2026-09-15T00:00:00+00:00")
    offset = replay.parse_utc("2026-09-15T02:00:00+02:00")

    assert naive == explicit == offset
    assert naive.tzinfo is not None
    assert int(naive.timestamp()) == 1789430400  # 2026-09-15T00:00:00Z exactly


# --- reconcile -------------------------------------------------------------

_KEYS = ("model", "init_time", "variable")
_TIMES = ("init_time",)


def test_key_hashes_ignore_dtype_and_datetime_resolution_differences() -> None:
    """pandas builds categoricals and nanosecond timestamps; Postgres returns
    plain strings and microsecond timestamps. The same natural key must hash
    identically either way, or every reconciliation would falsely mismatch."""
    moment = pd.Timestamp("2026-09-17T12:00:00", tz="UTC")
    from_transform = pd.DataFrame(
        {
            "model": pd.Categorical(["gfs"]),
            "init_time": pd.Series([moment]).astype("datetime64[ns, UTC]"),
            "variable": pd.Categorical(["temperature_2m"]),
        }
    )
    from_postgres = pd.DataFrame(
        {
            "model": ["gfs"],
            "init_time": pd.Series([moment]).astype("datetime64[us, UTC]"),
            "variable": ["temperature_2m"],
        }
    )

    assert (
        key_hashes(from_transform, _KEYS, _TIMES)[0] == key_hashes(from_postgres, _KEYS, _TIMES)[0]
    )


def test_key_hashes_distinguish_different_keys() -> None:
    frame = pd.DataFrame(
        {
            "model": ["gfs", "gfs", "icon"],
            "init_time": pd.to_datetime(
                ["2026-09-17T12:00", "2026-09-17T18:00", "2026-09-17T12:00"], utc=True
            ),
            "variable": ["t", "t", "t"],
        }
    )
    assert len(set(key_hashes(frame, _KEYS, _TIMES))) == 3


def test_diff_key_sets_reports_missing_and_extra() -> None:
    expected = np.array([1, 2, 3, 4], dtype=np.uint64)
    actual = np.array([3, 4, 5], dtype=np.uint64)

    assert diff_key_sets(expected, actual) == (2, 1)  # 1,2 missing; 5 extra
    assert diff_key_sets(expected, expected) == (0, 0)


def test_a_result_only_matches_when_nothing_is_missing_or_extra() -> None:
    def result(missing: int, extra: int) -> ReconciliationResult:
        return ReconciliationResult("t", 1, 1, 1, 0, 10, 10, missing, extra)

    assert result(0, 0).matched
    assert not result(1, 0).matched
    assert not result(0, 1).matched
