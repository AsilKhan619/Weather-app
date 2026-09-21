"""The health page's Kafka numbers against a real broker: consumer lag per group and topic,
dead-letter volume, and graceful degradation when Kafka cannot be reached."""

import time

import pandas as pd
import pytest
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.kafka import make_consumer, make_producer, produce_json
from nimbus.common.settings import Settings
from nimbus.dashboard.kafka_health import DLQ_TOPIC, kafka_health

pytestmark = pytest.mark.integration

FORECASTS = "weather.forecast.raw.v1"


def _lag(health_lag: pd.DataFrame, group: str, topic: str) -> int:
    row = health_lag[(health_lag["group"] == group) & (health_lag["topic"] == topic)]
    return int(row["lag"].iloc[0])


def test_lag_is_end_minus_committed_and_dlq_counts_dead_letters(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    producer = make_producer(settings)
    for i in range(5):
        produce_json(producer, FORECASTS, f"key-{i}", {"n": i})
    for i in range(2):
        produce_json(producer, DLQ_TOPIC, "k", {"bad": i})
    producer.flush(10)

    before = kafka_health(settings)
    assert before.error is None
    assert before.dlq_messages == 2
    # nobody has committed anything yet: every group is 5 behind on the forecast topic
    assert {_lag(before.lag, g, FORECASTS) for g in ("bronze-sink", "silver-forecast")} == {5}

    # silver-forecast reads and commits the whole topic
    consumer = make_consumer(settings, group_id="silver-forecast")
    consumer.subscribe([FORECASTS])
    seen, deadline = 0, time.monotonic() + 60
    while seen < 5 and time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is not None and not message.error():
            seen += 1
    assert seen == 5
    consumer.commit(asynchronous=False)
    consumer.close()

    after = kafka_health(settings)
    assert _lag(after.lag, "silver-forecast", FORECASTS) == 0
    assert _lag(after.lag, "bronze-sink", FORECASTS) == 5  # a different group is unaffected


def test_an_unreachable_broker_is_reported_not_raised() -> None:
    health = kafka_health(Settings(_env_file=None, kafka_bootstrap_servers="127.0.0.1:1"))

    assert health.error is not None
    assert health.lag.empty and health.dlq_messages is None
