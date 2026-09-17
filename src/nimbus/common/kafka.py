"""Thin, idempotent Kafka producer/consumer helpers (brief section 6). Micro-batch
consumer-group logic for bronze/silver lives in `nimbus.streaming` (Phase 1)."""

import json
import logging
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from nimbus.common.settings import Settings

logger = logging.getLogger(__name__)


class KafkaMessageLike(Protocol):
    """The subset of confluent_kafka.Message this project depends on.
    confluent_kafka.Message is a C-extension type with no public
    constructor, so streaming code is typed against this structural
    Protocol instead - it's what makes FakeMessage-based unit tests
    (no broker needed) type-check cleanly."""

    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def timestamp(self) -> tuple[int, int]: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def topic(self) -> str | None: ...


_DAY_MS = 24 * 60 * 60 * 1000

# Topic design per ADR 0001: partitions sized for a single-broker laptop
# setup with room for parallel bronze+silver consumer groups; retention short
# because the bronze Parquet lake, not Kafka, is the long-term replay source.
TOPIC_SPECS: dict[str, dict[str, int]] = {
    "weather.forecast.raw.v1": {"partitions": 6, "retention_ms": 7 * _DAY_MS},
    "weather.observation.raw.v1": {"partitions": 6, "retention_ms": 7 * _DAY_MS},
    "weather.dlq.v1": {"partitions": 3, "retention_ms": 30 * _DAY_MS},
    "weather.alert.v1": {"partitions": 3, "retention_ms": 14 * _DAY_MS},
    "weather.briefing.v1": {"partitions": 3, "retention_ms": 14 * _DAY_MS},
}


def ensure_topics(settings: Settings, timeout: float = 10.0) -> None:
    """Create any of TOPIC_SPECS that don't already exist. Idempotent - safe
    to call on every `make up`, not just once."""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    existing = admin.list_topics(timeout=timeout).topics
    missing = [name for name in TOPIC_SPECS if name not in existing]
    if not missing:
        logger.info("all topics already exist")
        return

    new_topics = [
        NewTopic(
            name,
            num_partitions=TOPIC_SPECS[name]["partitions"],
            replication_factor=1,
            config={"retention.ms": str(TOPIC_SPECS[name]["retention_ms"])},
        )
        for name in missing
    ]
    futures = admin.create_topics(new_topics)
    for name, future in futures.items():
        future.result(timeout=timeout)
        logger.info("created topic", extra={"topic": name})


def make_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def make_consumer(settings: Settings, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def delivery_callback(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("kafka delivery failed", extra={"kafka_error": str(err)})
    else:
        logger.debug(
            "kafka delivery succeeded",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )


def produce_json(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, default=str).encode("utf-8"),
        callback=delivery_callback,
    )
    producer.poll(0)
