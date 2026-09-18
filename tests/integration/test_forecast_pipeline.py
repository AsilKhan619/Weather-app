"""Phase 1 acceptance, end to end against real Kafka + Postgres: forecasts
are queryable in silver; re-processing the same data creates zero
duplicates; a malformed message lands in the DLQ without stopping the
consumer. Uses a recorded Open-Meteo fixture - never the live API."""

import json
from pathlib import Path

import httpx
import pytest
from confluent_kafka import Producer
from sqlalchemy import create_engine, text
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.config import Location, ModelsConfig, ModelSpec
from nimbus.common.kafka import make_consumer, make_producer
from nimbus.common.settings import Settings
from nimbus.ingestion.forecast_producer import produce_one_poll_cycle
from nimbus.streaming.bronze_sink import write_batch_to_parquet
from nimbus.streaming.forecast_silver import DLQ_TOPIC, SOURCE_TOPIC, process_batch
from nimbus.streaming.microbatch import GracefulShutdown, _collect_batch

FIXTURE = json.loads(
    (
        Path(__file__).resolve().parents[1] / "fixtures" / "open_meteo_forecast_response.json"
    ).read_text()
)


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


def _models() -> ModelsConfig:
    return ModelsConfig(
        models=[ModelSpec(id="gfs_seamless", name="NOAA GFS")],
        run_cadence_hours=6,
        run_lookback_steps=8,
        variables=["temperature_2m", "dew_point_2m", "wind_speed_10m", "pressure_msl"],
        forecast_days=1,
    )


def _fixture_handler(request: httpx.Request) -> httpx.Response:
    n_locations = len(request.url.params["latitude"].split(","))
    if n_locations == 1:
        return httpx.Response(200, json=FIXTURE)
    return httpx.Response(200, json=[FIXTURE for _ in range(n_locations)])


def _run_one_poll_and_produce(settings: Settings) -> int:
    producer = make_producer(settings)
    engine = create_engine(settings.postgres_dsn)
    with httpx.Client(transport=httpx.MockTransport(_fixture_handler)) as client:
        return produce_one_poll_cycle(client, producer, engine, _locations(), _models())


@pytest.mark.integration
def test_bronze_and_silver_fan_out_from_the_same_topic(
    stack: tuple[Settings, KafkaContainer, PostgresContainer], tmp_path: Path
) -> None:
    settings, _kafka, _pg = stack
    produced = _run_one_poll_and_produce(settings)
    assert produced == 2  # 2 locations x 1 model

    shutdown = GracefulShutdown()

    bronze_consumer = make_consumer(settings, group_id="bronze-sink")
    bronze_consumer.subscribe([SOURCE_TOPIC])
    bronze_batch = _collect_batch(bronze_consumer, 10, 20.0, shutdown)
    assert len(bronze_batch) == 2
    bronze_consumer.close()

    bronze_path = write_batch_to_parquet(bronze_batch, SOURCE_TOPIC, lake_root=tmp_path)
    assert bronze_path.exists()

    silver_consumer = make_consumer(settings, group_id="silver-forecast")
    dlq_producer = make_producer(settings)
    engine = create_engine(settings.postgres_dsn)
    silver_consumer.subscribe([SOURCE_TOPIC])
    silver_batch = _collect_batch(silver_consumer, 10, 20.0, shutdown)
    assert len(silver_batch) == 2  # bronze and silver are independent groups
    process_batch(silver_batch, engine, dlq_producer)
    silver_consumer.commit(asynchronous=False)
    silver_consumer.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.forecast")).scalar_one()
    # 2 locations x 24 hours x 4 variables
    assert row_count == 2 * 24 * 4


@pytest.mark.integration
def test_reprocessing_the_same_batch_creates_zero_duplicates(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    _run_one_poll_and_produce(settings)
    engine = create_engine(settings.postgres_dsn)
    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()

    consumer = make_consumer(settings, group_id="silver-forecast")
    consumer.subscribe([SOURCE_TOPIC])
    batch = _collect_batch(consumer, 10, 20.0, shutdown)
    process_batch(batch, engine, dlq_producer)
    consumer.close()  # deliberately don't commit - simulates redelivery

    consumer2 = make_consumer(settings, group_id="silver-forecast")
    consumer2.subscribe([SOURCE_TOPIC])
    batch2 = _collect_batch(consumer2, 10, 20.0, shutdown)
    assert len(batch2) == len(batch)  # same messages, redelivered
    process_batch(batch2, engine, dlq_producer)
    consumer2.commit(asynchronous=False)
    consumer2.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.forecast")).scalar_one()
    assert row_count == 2 * 24 * 4  # unchanged despite processing the batch twice


@pytest.mark.integration
def test_malformed_message_goes_to_dlq_without_blocking_the_batch(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    _run_one_poll_and_produce(settings)
    engine = create_engine(settings.postgres_dsn)

    # Inject one poison message directly onto the topic alongside the 2 real ones.
    raw_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    raw_producer.produce(SOURCE_TOPIC, key=b"poison", value=b"not valid json at all")
    raw_producer.flush(10)

    dlq_producer = make_producer(settings)
    shutdown = GracefulShutdown()
    consumer = make_consumer(settings, group_id="silver-forecast")
    consumer.subscribe([SOURCE_TOPIC])
    batch = _collect_batch(consumer, 10, 20.0, shutdown)
    assert len(batch) == 3

    process_batch(batch, engine, dlq_producer)
    consumer.commit(asynchronous=False)
    consumer.close()

    with engine.begin() as conn:
        row_count = conn.execute(text("SELECT count(*) FROM silver.forecast")).scalar_one()
    assert row_count == 2 * 24 * 4  # the 2 good messages still landed

    dlq_consumer = make_consumer(settings, group_id="dlq-check")
    dlq_consumer.subscribe([DLQ_TOPIC])
    dlq_batch = _collect_batch(dlq_consumer, 10, 20.0, shutdown)
    dlq_consumer.close()
    assert len(dlq_batch) == 1
    dlq_value = dlq_batch[0].value()
    assert dlq_value is not None
    dlq_record = json.loads(dlq_value)
    assert dlq_record["source_topic"] == SOURCE_TOPIC
    assert "error_type" in dlq_record and "error_message" in dlq_record
