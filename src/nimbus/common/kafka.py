"""Thin, idempotent Kafka producer/consumer helpers (brief section 6). Micro-batch
consumer-group logic for bronze/silver lives in `nimbus.streaming` (Phase 1)."""

import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer

from nimbus.common.settings import Settings

logger = logging.getLogger(__name__)


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
