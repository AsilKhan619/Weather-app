"""Phase 2 acceptance, end to end against real Kafka + Postgres: observations
are queryable in silver; reprocessing creates zero duplicates; a correction
(COR) overwrites the original; a malformed message lands in the DLQ without
stopping the consumer. Uses a fixed METAR fixture - never the live API."""

import json

import httpx
import pytest
from confluent_kafka import Producer
from sqlalchemy import create_engine, text
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.config import Location
from nimbus.common.kafka import make_consumer, make_producer
from nimbus.common.settings import Settings
from nimbus.ingestion.observation_producer import produce_one_poll_cycle
from nimbus.streaming.microbatch import GracefulShutdown, _collect_batch
from nimbus.streaming.observation_silver import DLQ_TOPIC, SOURCE_TOPIC, process_batch

_METAR_SFO = {
    "icaoId": "KSFO",
    "obsTime": 1789689360,
    "temp": 21.7,
    "dewp": 12.8,
    "wspd": 10,
    "slp": 1015.0,
    "rawOb": "METAR KSFO 172356Z 28010KT 10SM CLR 22/13 A3000 RMK AO2 SLP150",
}
_METAR_DEN = {
    "icaoId": "KDEN",
    "obsTime": 1789689180,
    "temp": 27.2,
    "dewp": 10.0,
    "wspd": 7,
    "slp": 1014.1,
    "rawOb": "METAR KDEN 172353Z 28007KT 10SM SCT080 27/10 A3018 RMK AO2 SLP141",
}


def _locations() -> list[Location]:
    return [
        Location(
            id="san-francisco",
            name="SF",
            climate="coastal",
            latitude=37.6213,
            longitude=-122.379,
            elevation_m=4,
            timezone="America/Los_Angeles",
            station="KSFO",
        ),
        Location(
            id="denver",
            name="Denver",
            climate="mountain",
            latitude=39.8561,
            longitude=-104.6737,
            elevation_m=1655,
            timezone="America/Denver",
            station="KDEN",
        ),
    ]


def _handler(reports: list[dict[str, object]]) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(200, json=reports))


def _run_one_poll_and_produce(settings: Settings, reports: list[dict[str, object]]) -> int:
    producer = make_producer(settings)
    engine = create_engine(settings.postgres_dsn)
    with httpx.Client(transport=_handler(reports)) as client:
        return produce_one_poll_cycle(client, producer, engine, _locations())


@pytest.mark.integration
def test_observations_land_in_silver_with_all_four_variables(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    produced = _run_one_poll_and_produce(settings, [_METAR_SFO, _METAR_DEN])
    assert produced == 2

    engine = create_engine(settings.postgres_dsn)
    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()
    consumer = make_consumer(settings, group_id="silver-observation")
    consumer.subscribe([SOURCE_TOPIC])
    batch = _collect_batch(consumer, 10, 20.0, shutdown)
    assert len(batch) == 2
    process_batch(batch, engine, dlq_producer)
    consumer.commit(asynchronous=False)
    consumer.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.observation")).scalar_one()
        stations = (
            conn.execute(text("SELECT DISTINCT station FROM silver.observation ORDER BY station"))
            .scalars()
            .all()
        )
    assert row_count == 2 * 4  # 2 stations x 4 variables
    assert stations == ["KDEN", "KSFO"]


@pytest.mark.integration
def test_reprocessing_the_same_report_creates_zero_duplicates(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    _run_one_poll_and_produce(settings, [_METAR_SFO])
    engine = create_engine(settings.postgres_dsn)
    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()

    consumer = make_consumer(settings, group_id="silver-observation")
    consumer.subscribe([SOURCE_TOPIC])
    batch = _collect_batch(consumer, 10, 20.0, shutdown)
    process_batch(batch, engine, dlq_producer)
    consumer.close()  # don't commit - simulates redelivery

    consumer2 = make_consumer(settings, group_id="silver-observation")
    consumer2.subscribe([SOURCE_TOPIC])
    batch2 = _collect_batch(consumer2, 10, 20.0, shutdown)
    assert len(batch2) == len(batch)
    process_batch(batch2, engine, dlq_producer)
    consumer2.commit(asynchronous=False)
    consumer2.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.observation")).scalar_one()
    assert row_count == 4  # unchanged despite processing twice


@pytest.mark.integration
def test_a_correction_overwrites_the_original_report(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    engine = create_engine(settings.postgres_dsn)
    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()

    _run_one_poll_and_produce(settings, [_METAR_SFO])
    consumer = make_consumer(settings, group_id="silver-observation")
    consumer.subscribe([SOURCE_TOPIC])
    process_batch(_collect_batch(consumer, 10, 20.0, shutdown), engine, dlq_producer)
    consumer.commit(asynchronous=False)
    consumer.close()

    original_raw_ob = str(_METAR_SFO["rawOb"])
    corrected = dict(_METAR_SFO, temp=99.0, rawOb=original_raw_ob.replace("Z 280", "Z COR 280"))
    _run_one_poll_and_produce(settings, [corrected])
    consumer2 = make_consumer(settings, group_id="silver-observation")
    consumer2.subscribe([SOURCE_TOPIC])
    process_batch(_collect_batch(consumer2, 10, 20.0, shutdown), engine, dlq_producer)
    consumer2.commit(asynchronous=False)
    consumer2.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.observation")).scalar_one()
        temp_row = conn.execute(
            text(
                "SELECT value, is_corrected FROM silver.observation "
                "WHERE station = 'KSFO' AND variable = 'temperature_2m'"
            )
        ).one()
    assert row_count == 4  # still one row per variable - the COR overwrote, didn't add
    assert temp_row.is_corrected is True
    assert temp_row.value == pytest.approx(99.0 + 273.15)


@pytest.mark.integration
def test_malformed_message_goes_to_dlq_without_blocking_the_batch(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    _run_one_poll_and_produce(settings, [_METAR_SFO])
    engine = create_engine(settings.postgres_dsn)

    raw_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    raw_producer.produce(SOURCE_TOPIC, key=b"poison", value=b"not valid json at all")
    raw_producer.flush(10)

    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()
    consumer = make_consumer(settings, group_id="silver-observation")
    consumer.subscribe([SOURCE_TOPIC])
    batch = _collect_batch(consumer, 10, 20.0, shutdown)
    assert len(batch) == 2

    process_batch(batch, engine, dlq_producer)
    consumer.commit(asynchronous=False)
    consumer.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.observation")).scalar_one()
    assert row_count == 4  # the 1 good report still landed (4 variables)

    dlq_consumer = make_consumer(settings, group_id="dlq-check")
    dlq_consumer.subscribe([DLQ_TOPIC])
    dlq_batch = _collect_batch(dlq_consumer, 10, 20.0, shutdown)
    dlq_consumer.close()
    assert len(dlq_batch) == 1
    dlq_value = dlq_batch[0].value()
    assert dlq_value is not None
    dlq_record = json.loads(dlq_value)
    assert dlq_record["source_topic"] == SOURCE_TOPIC
