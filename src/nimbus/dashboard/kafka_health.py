"""Consumer lag and dead-letter volume for the health page, via the Kafka AdminClient.

The dashboard must keep working when Kafka is down (the data already in Postgres is still
worth looking at), so every failure becomes a message in `KafkaHealth.error` rather than an
exception."""

from dataclasses import dataclass, field

import pandas as pd
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from nimbus.common.settings import Settings

DLQ_TOPIC = "weather.dlq.v1"
# The long-running consumers and the topics each one reads.
GROUPS: dict[str, list[str]] = {
    "bronze-sink": ["weather.forecast.raw.v1", "weather.observation.raw.v1"],
    "silver-forecast": ["weather.forecast.raw.v1"],
    "silver-observation": ["weather.observation.raw.v1"],
    "alert-detector": ["weather.forecast.raw.v1", "weather.observation.raw.v1"],
}
_TIMEOUT = 5.0


@dataclass
class KafkaHealth:
    lag: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=["group", "topic", "committed", "end", "lag"])
    )
    dlq_messages: int | None = None
    error: str | None = None


def _partitions(consumer: Consumer, topic: str) -> list[TopicPartition]:
    metadata = consumer.list_topics(topic, timeout=_TIMEOUT)
    return [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]


def kafka_health(settings: Settings) -> KafkaHealth:
    """Lag per (group, topic) = end offset - committed offset, summed over partitions; a
    group that has never committed on a topic shows its whole backlog."""
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        probe = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "dashboard-probe",
                "enable.auto.commit": False,
            }
        )
        try:
            rows: list[dict[str, object]] = []
            ends: dict[str, dict[int, tuple[int, int]]] = {}
            for topic in {t for topics in GROUPS.values() for t in topics} | {DLQ_TOPIC}:
                ends[topic] = {}
                for tp in _partitions(probe, topic):
                    low, high = probe.get_watermark_offsets(tp, timeout=_TIMEOUT)
                    ends[topic][tp.partition] = (low, high)

            for group, topics in GROUPS.items():
                request = ConsumerGroupTopicPartitions(
                    group, [TopicPartition(t, p) for t in topics for p in ends[t]]
                )
                committed = admin.list_consumer_group_offsets([request])[group].result(_TIMEOUT)
                by_partition = {
                    (tp.topic, tp.partition): tp.offset for tp in committed.topic_partitions
                }
                for topic in topics:
                    end = committed_total = lag = 0
                    for partition, (low, high) in ends[topic].items():
                        offset = by_partition.get((topic, partition), -1)
                        position = low if offset < 0 else offset  # -1: nothing committed yet
                        end += high
                        committed_total += position
                        lag += max(high - position, 0)
                    rows.append(
                        {
                            "group": group,
                            "topic": topic,
                            "committed": committed_total,
                            "end": end,
                            "lag": lag,
                        }
                    )
            dlq = sum(high - low for low, high in ends[DLQ_TOPIC].values())
            return KafkaHealth(pd.DataFrame(rows), dlq_messages=dlq)
        finally:
            probe.close()
    except Exception as exc:  # the page reports this; it must not take the dashboard down
        return KafkaHealth(error=f"{type(exc).__name__}: {exc}")
