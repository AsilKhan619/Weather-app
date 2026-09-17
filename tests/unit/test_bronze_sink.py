from pathlib import Path

import pyarrow.parquet as pq

from nimbus.streaming.bronze_sink import write_batch_to_parquet


class FakeMessage:
    """Duck-typed stand-in for confluent_kafka.Message (a C-extension type
    with no public constructor) so this stays a real, dependency-free unit
    test rather than needing a live broker."""

    def __init__(
        self,
        partition: int,
        offset: int,
        ts_ms: int,
        key: bytes,
        value: bytes,
        topic: str = "test-topic",
    ) -> None:
        self._partition = partition
        self._offset = offset
        self._ts_ms = ts_ms
        self._key = key
        self._value = value
        self._topic = topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return (1, self._ts_ms)

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic


def test_writes_one_parquet_file_with_lineage_columns(tmp_path: Path) -> None:
    messages = [
        FakeMessage(0, 42, 1_700_000_000_000, b"san-francisco", b'{"event_id": "a"}'),
        FakeMessage(1, 7, 1_700_000_001_000, b"denver", b'{"event_id": "b"}'),
    ]

    path = write_batch_to_parquet(messages, "weather.forecast.raw.v1", lake_root=tmp_path)

    assert path.exists()
    assert path.parent.name.startswith("dt=")
    assert path.parent.parent.name == "weather.forecast.raw.v1"

    table = pq.read_table(path)
    assert table.num_rows == 2
    assert set(table.column_names) == {
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp_ms",
        "key",
        "value",
    }
    assert table.column("kafka_partition").to_pylist() == [0, 1]
    assert table.column("kafka_offset").to_pylist() == [42, 7]
    assert table.column("key").to_pylist() == [b"san-francisco", b"denver"]


def test_batches_from_different_topics_are_kept_separate(tmp_path: Path) -> None:
    forecast_msg = FakeMessage(0, 1, 0, b"k", b"v")

    forecast_path = write_batch_to_parquet(
        [forecast_msg], "weather.forecast.raw.v1", lake_root=tmp_path
    )
    observation_path = write_batch_to_parquet(
        [forecast_msg], "weather.observation.raw.v1", lake_root=tmp_path
    )

    assert forecast_path != observation_path
    assert "weather.forecast.raw.v1" in str(forecast_path)
    assert "weather.observation.raw.v1" in str(observation_path)
