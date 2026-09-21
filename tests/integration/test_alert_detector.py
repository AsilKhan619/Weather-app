"""Phase 4a acceptance, against real Kafka + Postgres: a recorded forecast event that
moves a location's forecast raises an alert on `weather.alert.v1` within a minute of being
produced, exactly once even when the event is replayed; and the insert-then-publish
delivery recovers from a crash between the two steps without losing or duplicating."""

import json
import os
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import Engine, create_engine, text
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.alerts.detector import (
    ALERT_TOPIC,
    FORECAST_TOPIC,
    OBSERVATION_TOPIC,
    Detector,
    process_batch,
    publish_batch,
)
from nimbus.alerts.store import PostgresForecasts, insert_alerts
from nimbus.common.config import (
    Location,
    load_alerts_config,
    load_locations,
    load_models_config,
)
from nimbus.common.db import make_engine
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import make_consumer, make_producer, produce_json
from nimbus.common.schemas import AlertPayload, ForecastRawPayload, ObservationRawPayload
from nimbus.common.settings import Settings
from nimbus.jobs.load_dimensions import load_dimensions
from nimbus.streaming import forecast_silver
from nimbus.streaming.microbatch import GracefulShutdown, run_microbatch_loop

pytestmark = pytest.mark.integration

INIT = datetime(2026, 9, 20, 12, tzinfo=UTC)
LOCATION: Location = load_locations()[0]


class _Stop(GracefulShutdown):
    """A shutdown flag without signal handlers (those can only be set on the main thread)."""

    def __init__(self) -> None:
        self.should_stop = False


def _forecast_event(
    *, temp_c: float, init: datetime, model: str = "gfs_seamless"
) -> dict[str, Any]:
    hours = range(1, 73)
    hourly = {
        "time": [(init + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in hours],
        "temperature_2m": [temp_c] * 72,
        "dew_point_2m": [10.0] * 72,
        "wind_speed_10m": [18.0] * 72,
        "pressure_msl": [1013.0] * 72,
    }
    envelope = EventEnvelope[ForecastRawPayload](
        event_id=f"recorded-{model}-{init:%Y%m%dT%H}-{temp_c}",
        source="forecast_producer",
        event_type="forecast.raw",
        produced_at=datetime.now(UTC),
        ingestion_mode="live",
        payload=ForecastRawPayload(
            model=model, location_id=LOCATION.id, run=init, api_response={"hourly": hourly}
        ),
    )
    return envelope.model_dump(mode="json")


def _detector(engine: Engine) -> Detector:
    models = load_models_config()
    return Detector(
        PostgresForecasts(engine),
        load_alerts_config(),
        load_locations(),
        [m.id for m in models.models],
        models.run_cadence_hours,
    )


def _alerts_on_topic(settings: Settings, group: str, wait: float) -> list[dict[str, Any]]:
    consumer: Consumer = make_consumer(settings, group_id=group)
    consumer.subscribe([ALERT_TOPIC])
    found: list[dict[str, Any]] = []
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())
        found.append(json.loads(message.value() or b"{}"))
    consumer.close()
    return found


def test_a_replayed_forecast_event_raises_one_alert_within_a_minute(
    stack: tuple[Settings, KafkaContainer, PostgresContainer],
) -> None:
    settings, _kafka, _pg = stack
    engine = make_engine(settings)
    load_dimensions(engine)

    # The previous live run (20 degC) is already in silver, as the silver consumer left it.
    previous = _forecast_event(temp_c=20.0, init=INIT - timedelta(hours=6))
    forecast_silver.upsert_forecast_rows(
        engine, forecast_silver.message_to_frame(json.dumps(previous).encode())
    )

    # Start the detector as a long-running service and wait until it owns its partitions.
    consumer = make_consumer(settings, group_id="alert-detector-test")
    consumer.subscribe([FORECAST_TOPIC, OBSERVATION_TOPIC])
    deadline = time.monotonic() + 90
    while not consumer.assignment() and time.monotonic() < deadline:
        consumer.poll(0.5)
    assert consumer.assignment(), "detector never got its partitions"

    detector, producer, stop = _detector(engine), make_producer(settings), _Stop()
    service = threading.Thread(
        target=run_microbatch_loop,
        args=(consumer, [FORECAST_TOPIC, OBSERVATION_TOPIC]),
        kwargs={
            "handle_batch": lambda batch: process_batch(batch, detector, engine, producer),
            "max_batch_seconds": 1.0,
            "shutdown": stop,
        },
        daemon=True,
    )
    service.start()

    # Replay a recorded event whose forecast is 7 K warmer than the previous run's.
    trigger = _forecast_event(temp_c=27.0, init=INIT)
    started = time.monotonic()
    produce_json(producer, FORECAST_TOPIC, LOCATION.id, trigger)
    producer.flush(10)

    latency = None
    while time.monotonic() - started < 60:
        with engine.connect() as conn:
            published = conn.execute(
                text("SELECT count(*) FROM gold.alert WHERE published_at IS NOT NULL")
            ).scalar_one()
        if published:
            latency = time.monotonic() - started
            break
        time.sleep(0.25)
    assert latency is not None, "no alert within a minute"
    print(f"\nalert latency (produce -> published): {latency:.2f}s")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:  # make the measurement visible on the CI run page
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write(f"- alert latency, recorded event to published alert: {latency:.2f} s\n")

    # The same event, replayed: the detector re-detects it but must not publish it again.
    produce_json(producer, FORECAST_TOPIC, LOCATION.id, trigger)
    producer.flush(10)
    time.sleep(6)
    stop.should_stop = True
    service.join(timeout=30)

    on_topic = _alerts_on_topic(settings, "alert-reader", wait=10)
    assert len(on_topic) == 1
    payload = on_topic[0]["payload"]
    assert (payload["rule"], payload["severity"], payload["variable"]) == (
        "run_change",
        "critical",
        "temperature_2m",
    )
    assert payload["location_id"] == LOCATION.id and payload["model"] == "gfs_seamless"
    assert payload["metric"] == pytest.approx(7.0)
    assert on_topic[0]["event_id"] == payload["alert_id"]
    assert payload["triggered_by_event_id"] == trigger["event_id"]

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT alert_id, published_at, details FROM gold.alert")).all()
    assert len(rows) == 1 and rows[0].published_at is not None
    assert rows[0].details["hours_compared"] == 48
    assert latency < 60


# --- delivery: insert, then publish, then mark ------------------------------------------


@pytest.fixture
def engine(pg_settings: Settings) -> Iterator[Engine]:
    eng = create_engine(pg_settings.postgres_dsn)
    with eng.begin() as conn:
        conn.execute(text("TRUNCATE gold.alert, silver.forecast"))
    yield eng
    eng.dispose()


def _alert(alert_id: str = "a1") -> AlertPayload:
    return AlertPayload(
        alert_id=alert_id,
        rule="run_change",
        severity="warning",
        location_id="denver",
        variable="temperature_2m",
        model="gfs_seamless",
        event_time=INIT,
        metric=4.0,
        threshold=3.0,
        details={"hours_compared": 48},
        triggered_by_event_id="evt",
        detected_at=INIT,
    )


def _producer(delivered: int = 0) -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = delivered  # messages still undelivered
    return producer


def test_an_alert_is_published_once_and_marked(engine: Engine) -> None:
    producer = _producer()

    first = publish_batch(engine, producer, [_alert()])
    again = publish_batch(engine, producer, [_alert()])

    assert [a.alert_id for a in first] == ["a1"] and again == []
    assert producer.produce.call_count == 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT published_at FROM gold.alert")).scalar_one() is not None


def test_a_crash_after_the_insert_is_retried_and_not_lost(engine: Engine) -> None:
    insert_alerts(engine, [_alert()])  # the process died right here: stored, never published

    sent = publish_batch(engine, _producer(), [_alert()])  # the restart re-detects the event

    assert [a.alert_id for a in sent] == ["a1"]
    with engine.connect() as conn:
        assert conn.execute(text("SELECT published_at FROM gold.alert")).scalar_one() is not None


def test_undelivered_alerts_stay_unpublished_and_the_batch_fails(engine: Engine) -> None:
    with pytest.raises(RuntimeError, match="not confirmed"):
        publish_batch(engine, _producer(delivered=1), [_alert()])

    with engine.connect() as conn:  # so the next attempt sends it again
        assert conn.execute(text("SELECT published_at FROM gold.alert")).scalar_one() is None


def test_one_batch_with_the_same_alert_twice_sends_it_once(engine: Engine) -> None:
    producer = _producer()
    publish_batch(engine, producer, [_alert("dup"), _alert("dup"), _alert("other")])
    assert producer.produce.call_count == 2


def test_unreadable_messages_are_skipped_without_dropping_the_batch(engine: Engine) -> None:
    """process_batch skips what is not an event it can read, without dropping the batch."""
    load_dimensions(engine)

    class _Msg:
        def __init__(self, topic: str, value: bytes | None) -> None:
            self._topic, self._value = topic, value

        def value(self) -> bytes | None:
            return self._value

        def topic(self) -> str:
            return self._topic

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return 0

        def timestamp(self) -> tuple[int, int]:
            return (1, 0)

        def key(self) -> bytes | None:
            return None

    envelope = EventEnvelope[ObservationRawPayload](
        event_id="obs",
        source="observation_producer",
        event_type="observation.raw",
        produced_at=INIT,
        ingestion_mode="live",
        payload=ObservationRawPayload(
            station=LOCATION.station,
            observed_at=INIT,
            api_response={"temp": 20.0, "dewp": 10.0, "wspd": 5, "slp": 1013.0, "rawOb": "M"},
        ),
    )
    batch = [
        _Msg(OBSERVATION_TOPIC, b"not json"),
        _Msg(OBSERVATION_TOPIC, None),
        _Msg(OBSERVATION_TOPIC, envelope.model_dump_json().encode()),
    ]

    sent = process_batch(batch, _detector(engine), engine, _producer())

    assert sent == []  # no forecast in silver to miss: nothing to say, and no crash


# --- the silver lookups the detector relies on ------------------------------------------


def _load_run(
    engine: Engine, *, model: str, init: datetime, temp_c: float, mode: str = "live"
) -> None:
    event = _forecast_event(temp_c=temp_c, init=init, model=model)
    event["ingestion_mode"] = mode
    forecast_silver.upsert_forecast_rows(
        engine, forecast_silver.message_to_frame(json.dumps(event).encode())
    )


def test_previous_run_is_the_latest_live_run_before_and_ignores_backfill(engine: Engine) -> None:
    store = PostgresForecasts(engine)
    _load_run(engine, model="gfs_seamless", init=INIT - timedelta(hours=12), temp_c=10.0)
    _load_run(engine, model="gfs_seamless", init=INIT - timedelta(hours=6), temp_c=20.0)
    _load_run(
        engine, model="gfs_seamless", init=INIT - timedelta(hours=3), temp_c=30.0, mode="backfill"
    )

    found = store.previous_run("gfs_seamless", LOCATION.id, INIT, 48)

    assert found is not None
    previous_init, frame = found
    assert previous_init == INIT - timedelta(hours=6)  # not the older run, not the backfilled one
    temperatures = frame[frame["variable"] == "temperature_2m"]["value"]
    assert len(temperatures) == 48 and temperatures.iloc[0] == pytest.approx(293.15)
    assert store.previous_run("gfs_seamless", LOCATION.id, INIT - timedelta(hours=12), 48) is None
    assert store.previous_run("icon_seamless", LOCATION.id, INIT, 48) is None


def test_latest_run_skips_a_model_that_is_a_whole_cycle_behind(engine: Engine) -> None:
    store = PostgresForecasts(engine)
    _load_run(engine, model="icon_seamless", init=INIT - timedelta(hours=6), temp_c=15.0)
    _load_run(engine, model="ecmwf_ifs025", init=INIT - timedelta(hours=12), temp_c=15.0)

    assert store.latest_run("icon_seamless", LOCATION.id, INIT, 48, 6) is not None
    assert store.latest_run("ecmwf_ifs025", LOCATION.id, INIT, 48, 6) is None


def test_forecasts_at_returns_each_models_latest_short_range_forecast(engine: Engine) -> None:
    store = PostgresForecasts(engine)
    target = INIT + timedelta(hours=10)
    _load_run(engine, model="gfs_seamless", init=INIT - timedelta(hours=6), temp_c=10.0)  # lead 16
    _load_run(engine, model="gfs_seamless", init=INIT, temp_c=12.0)  # lead 10: the one to use
    _load_run(
        engine, model="icon_seamless", init=INIT - timedelta(hours=24), temp_c=99.0
    )  # lead 34
    _load_run(engine, model="ecmwf_ifs025", init=INIT, temp_c=14.0, mode="backfill")

    got = store.forecasts_at(LOCATION.id, "temperature_2m", target, 24)

    assert got == {"gfs_seamless": pytest.approx(285.15)}  # icon too long-range, ecmwf backfilled
