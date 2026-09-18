"""Read the bronze Parquet lake back as message-shaped objects (brief section 6:
rebuild silver from bronze once Kafka retention has passed).

`BronzeMessage` satisfies `KafkaMessageLike`, so the silver `load_messages`
functions - the exact code the live consumers run - process replayed messages
with no broker involved and no second implementation to keep in sync."""

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DEFAULT_LAKE_ROOT = Path("data/lake/bronze")


@dataclass(frozen=True)
class BronzeMessage:
    topic_name: str
    partition_id: int
    offset_value: int
    timestamp_ms: int
    key_bytes: bytes | None
    value_bytes: bytes | None

    def partition(self) -> int | None:
        return self.partition_id

    def offset(self) -> int | None:
        return self.offset_value

    def timestamp(self) -> tuple[int, int]:
        return (1, self.timestamp_ms)  # 1 = CreateTime, mirroring Kafka's tuple

    def key(self) -> bytes | None:
        return self.key_bytes

    def value(self) -> bytes | None:
        return self.value_bytes

    def topic(self) -> str | None:
        return self.topic_name


def list_bronze_files(topic: str, lake_root: Path = DEFAULT_LAKE_ROOT) -> list[Path]:
    """Every Parquet file for a topic in write order. Filenames start with a UTC
    timestamp, so sorting by name reproduces arrival order across partitions and
    days (dt=YYYY-MM-DD/ directories sort chronologically too)."""
    topic_dir = lake_root / topic
    if not topic_dir.exists():
        return []
    return sorted(topic_dir.rglob("*.parquet"), key=lambda p: (p.parent.name, p.name))


def iter_bronze_batches(
    topic: str, lake_root: Path = DEFAULT_LAKE_ROOT, batch_size: int = 50
) -> Iterator[list[BronzeMessage]]:
    """Yield messages in bounded batches, one file at a time (never the whole
    lake in memory). Within a file, order is (partition, offset) - the order the
    sink received them."""
    for path in list_bronze_files(topic, lake_root):
        rows = pq.read_table(path).to_pylist()
        rows.sort(key=lambda r: (r["kafka_partition"] or 0, r["kafka_offset"] or 0))
        messages = [
            BronzeMessage(
                topic_name=topic,
                partition_id=row["kafka_partition"] or 0,
                offset_value=row["kafka_offset"] or 0,
                timestamp_ms=row["kafka_timestamp_ms"] or 0,
                key_bytes=row["key"],
                value_bytes=row["value"],
            )
            for row in rows
        ]
        for start in range(0, len(messages), batch_size):
            yield messages[start : start + batch_size]
