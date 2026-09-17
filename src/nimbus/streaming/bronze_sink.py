"""Bronze sink consumer: writes raw events verbatim to the Parquet lake
(brief section 7), one file per micro-batch (avoids the small-file problem),
carrying Kafka partition/offset/timestamp for lineage (`make trace`, Phase 3)."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from nimbus.common.kafka import KafkaMessageLike, make_consumer
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.streaming.microbatch import run_microbatch_loop

logger = logging.getLogger(__name__)

DEFAULT_LAKE_ROOT = Path("data/lake/bronze")


def write_batch_to_parquet(
    messages: Sequence[KafkaMessageLike], topic: str, lake_root: Path = DEFAULT_LAKE_ROOT
) -> Path:
    """One Parquet file per batch, partitioned by UTC ingestion date. The
    `value` column is the raw event JSON bytes, untouched - bronze mirrors
    what was on the topic, transformation happens in silver."""
    dt = datetime.now(UTC).strftime("%Y-%m-%d")
    partition_dir = lake_root / topic / f"dt={dt}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    table = pa.table(
        {
            "kafka_partition": [msg.partition() for msg in messages],
            "kafka_offset": [msg.offset() for msg in messages],
            "kafka_timestamp_ms": [msg.timestamp()[1] for msg in messages],
            "key": [msg.key() for msg in messages],
            "value": [msg.value() for msg in messages],
        }
    )
    file_path = partition_dir / f"{uuid4()}.parquet"
    pq.write_table(table, file_path)
    logger.info(
        "wrote bronze batch",
        extra={"topic": topic, "rows": len(messages), "path": str(file_path)},
    )
    return file_path


def main(topics: list[str] | None = None) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    topics = topics or ["weather.forecast.raw.v1"]
    consumer = make_consumer(settings, group_id="bronze-sink")

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        by_topic: dict[str, list[KafkaMessageLike]] = {}
        for msg in messages:
            topic_name = msg.topic()
            assert topic_name is not None
            by_topic.setdefault(topic_name, []).append(msg)
        for topic, topic_messages in by_topic.items():
            write_batch_to_parquet(topic_messages, topic)

    run_microbatch_loop(consumer, topics, handle_batch)


if __name__ == "__main__":
    main()
