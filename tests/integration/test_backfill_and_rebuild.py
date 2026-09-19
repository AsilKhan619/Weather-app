"""Phase 2 acceptance, end to end against real Kafka + Postgres:

  * historical forecasts and observations flow through the same topics and land
    in the same silver tables as live data, distinguished only by ingestion_mode;
  * `--drain` consumers finish on their own once caught up;
  * reconciliation reports produced -> bronze -> silver as matching;
  * silver rebuilt from the bronze lake is identical to the original, and
    reconciliation detects both missing and unexplained rows.

One stack and one load are shared by the module (it is the expensive part), so
each test leaves the data consistent for the next. Recorded/mocked HTTP only."""

import json
from collections.abc import Callable, Iterator, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
from confluent_kafka import Producer
from sqlalchemy import Engine, create_engine, text
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.config import Location, ModelsConfig, ModelSpec
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer
from nimbus.common.settings import Settings
from nimbus.ingestion.backfill import backfill_forecasts, backfill_observations
from nimbus.ingestion.forecast_producer import produce_one_poll_cycle as produce_live_forecasts
from nimbus.ingestion.observation_producer import produce_one_poll_cycle as produce_live_metars
from nimbus.jobs.reconcile import TOPIC_SPECS, reconcile_topic
from nimbus.jobs.replay import UnsafeRebuildError, replay_from_bronze
from nimbus.streaming import forecast_silver, observation_silver
from nimbus.streaming.bronze_sink import write_batch_to_parquet
from nimbus.streaming.microbatch import run_microbatch_loop

Stack = tuple[Settings, KafkaContainer, PostgresContainer]

FORECAST_TOPIC = "weather.forecast.raw.v1"
OBSERVATION_TOPIC = "weather.observation.raw.v1"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
LIVE_FORECAST = json.loads((FIXTURES / "open_meteo_forecast_response.json").read_text())

# 2 days of hourly valid times for the Previous Runs mock
_HOURS = [f"2024-06-0{day}T{hour:02d}:00" for day in (1, 2) for hour in range(24)]
_IEM_CSV = (
    "station,valid,tmpc,dwpc,sknt,mslp,alti,metar\n"
    "X,2024-06-01 00:56,17.78,10.00,18.00,1010.80,29.85,{st} 010056Z 28018KT 10SM 18/10 A2985\n"
    "X,2024-06-01 01:56,16.11,9.44,14.00,1010.90,29.85,{st} 010156Z 30014KT 10SM 16/09 A2985\n"
)
_LIVE_METAR = {
    "icaoId": "KSFO",
    "obsTime": 1789689360,
    "temp": 21.7,
    "dewp": 12.8,
    "wspd": 10,
    "slp": 1015.0,
    "rawOb": "METAR KSFO 172356Z 28010KT 10SM CLR 22/13 A3000 RMK AO2 SLP150",
}


def _locations() -> list[Location]:
    def loc(id_: str, station: str, lat: float, lon: float) -> Location:
        return Location(
            id=id_,
            name=id_,
            climate="x",
            latitude=lat,
            longitude=lon,
            elevation_m=1,
            timezone="UTC",
            station=station,
        )

    return [loc("san-francisco", "KSFO", 37.6, -122.4), loc("denver", "KDEN", 39.9, -104.7)]


def _models() -> ModelsConfig:
    return ModelsConfig(
        models=[ModelSpec(id="gfs_seamless", name="GFS")],
        run_cadence_hours=6,
        run_lookback_steps=8,
        variables=["temperature_2m", "dew_point_2m", "wind_speed_10m", "pressure_msl"],
        forecast_days=1,
        backfill_lead_days=[1, 2],
    )


def _previous_runs_api(request: httpx.Request) -> httpx.Response:
    n = len(request.url.params["latitude"].split(","))
    body = {
        "hourly": {
            "time": _HOURS,
            "temperature_2m_previous_day1": [10.0 + i * 0.1 for i in range(48)],
            "temperature_2m_previous_day2": [9.0 + i * 0.1 for i in range(48)],
            "wind_speed_10m_previous_day1": [None] + [18.0] * 47,  # one missing hour
        }
    }
    return httpx.Response(200, json=[body] * n if n > 1 else body)


def _live_forecast_api(request: httpx.Request) -> httpx.Response:
    n = len(request.url.params["latitude"].split(","))
    return httpx.Response(200, json=LIVE_FORECAST if n == 1 else [LIVE_FORECAST] * n)


def _iem_api(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=_IEM_CSV.format(st=request.url.params["station"]))


def _drain(settings: Settings, group: str, topics: list[str], handler: Callable[..., None]) -> None:
    """Run a real consumer until caught up - exercises --drain against Kafka."""
    run_microbatch_loop(
        make_consumer(settings, group),
        topics,
        handler,
        max_batch_seconds=2.0,
        drain=True,
    )


class Loaded:
    def __init__(self, settings: Settings, engine: Engine, lake: Path) -> None:
        self.settings = settings
        self.engine = engine
        self.lake = lake


@pytest.fixture(scope="module")
def loaded(module_stack: Stack, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Loaded]:
    settings, _kafka, _pg = module_stack
    engine = create_engine(settings.postgres_dsn)
    lake = tmp_path_factory.mktemp("lake")
    locations = _locations()
    producer = make_producer(settings)

    # --- produce: live + backfill + one poison message per topic -------------
    with httpx.Client(transport=httpx.MockTransport(_live_forecast_api)) as client:
        produce_live_forecasts(client, producer, engine, locations, _models())
    with httpx.Client(transport=httpx.MockTransport(_previous_runs_api)) as client:
        backfill_forecasts(
            client,
            producer,
            locations,
            _models(),
            date(2024, 6, 1),
            date(2024, 6, 2),
            sleep=lambda s: None,
        )
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[_LIVE_METAR]))
    ) as client:
        produce_live_metars(client, producer, engine, locations)
    with httpx.Client(transport=httpx.MockTransport(_iem_api)) as client:
        backfill_observations(
            client, producer, locations, date(2024, 6, 1), date(2024, 6, 1), sleep=lambda s: None
        )
    for topic in (FORECAST_TOPIC, OBSERVATION_TOPIC):
        producer.produce(topic, key=b"poison", value=b"not valid json at all")
    producer.flush(30)

    # --- consume: bronze sink, then both silver consumers, all draining ------
    def to_lake(messages: Sequence[KafkaMessageLike]) -> None:
        by_topic: dict[str, list[KafkaMessageLike]] = {}
        for message in messages:
            by_topic.setdefault(message.topic() or "", []).append(message)
        for topic, batch in by_topic.items():
            write_batch_to_parquet(batch, topic, lake_root=lake)

    dlq = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    _drain(settings, "bronze-sink", [FORECAST_TOPIC, OBSERVATION_TOPIC], to_lake)
    _drain(
        settings,
        "silver-forecast",
        [FORECAST_TOPIC],
        lambda m: forecast_silver.process_batch(m, engine, dlq),
    )
    _drain(
        settings,
        "silver-observation",
        [OBSERVATION_TOPIC],
        lambda m: observation_silver.process_batch(m, engine, dlq),
    )
    yield Loaded(settings, engine, lake)


def _rows(engine: Engine, sql: str) -> list[tuple[Any, ...]]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(sql)).all()]


def _scalar(engine: Engine, sql: str) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


def _snapshot(engine: Engine) -> list[tuple[Any, ...]]:
    with engine.connect() as conn:
        forecast = conn.execute(
            text(
                "SELECT model, location_id, init_time, valid_time, variable, value, lead_hours, "
                "ingestion_mode, source_event_id FROM silver.forecast ORDER BY 1,2,3,4,5"
            )
        ).all()
        observation = conn.execute(
            text(
                "SELECT station, observed_at, variable, value, raw_text, is_corrected, "
                "ingestion_mode, source_event_id FROM silver.observation ORDER BY 1,2,3"
            )
        ).all()
    return [tuple(r) for r in forecast] + [tuple(r) for r in observation]


@pytest.mark.integration
def test_live_and_backfilled_data_share_the_same_tables(loaded: Loaded) -> None:
    engine = loaded.engine

    # forecasts: backfill = 2 locations x 3 columns x 48 hours; live = 2 x 24 hours x 4 vars
    modes = dict(_rows(engine, "SELECT ingestion_mode, count(*) FROM silver.forecast GROUP BY 1"))
    assert modes == {"backfill": 2 * 3 * 48, "live": 2 * 24 * 4}

    # missing values must be SQL NULL, never NaN: 'NaN'::float8 passes IS NOT NULL
    # and would turn avg() over the column into NaN (Phase 3 accuracy metrics)
    assert _scalar(engine, "SELECT count(*) FROM silver.forecast WHERE value = 'NaN'") == 0
    assert _scalar(engine, "SELECT count(*) FROM silver.forecast WHERE value IS NULL") == 2
    # avg() over the backfilled wind column ignores the NULL: 94 hours of 18 km/h = 5 m/s.
    # (It would be NaN if the missing hour had been stored as NaN.)
    backfilled_wind_avg = _scalar(
        engine,
        "SELECT avg(value) FROM silver.forecast "
        "WHERE variable = 'wind_speed_10m' AND ingestion_mode = 'backfill'",
    )
    assert backfilled_wind_avg == pytest.approx(5.0)

    # the backfill's derived init_time = valid_time - lead offset
    derived = _scalar(
        engine,
        "SELECT count(*) FROM silver.forecast WHERE ingestion_mode = 'backfill' "
        "AND valid_time - init_time = make_interval(hours => lead_hours)",
    )
    assert derived == 2 * 3 * 48

    # observations: 2 stations x 2 IEM reports x 4 vars + 1 live report x 4 vars
    obs_modes = dict(
        _rows(engine, "SELECT ingestion_mode, count(*) FROM silver.observation GROUP BY 1")
    )
    assert obs_modes == {"backfill": 2 * 2 * 4, "live": 4}


@pytest.mark.integration
def test_reconciliation_matches_and_accounts_for_the_poison_message(loaded: Loaded) -> None:
    for spec in TOPIC_SPECS.values():
        result = reconcile_topic(loaded.engine, spec, loaded.lake)

        assert result.matched, result
        assert result.bronze_unparseable == 1  # the poison message went to the DLQ
        assert result.expected_silver_rows == result.silver_rows
        # every bronze message is either a distinct event or the one poison message
        assert result.bronze_messages == result.bronze_distinct_events + result.bronze_unparseable

    forecast = reconcile_topic(loaded.engine, TOPIC_SPECS[FORECAST_TOPIC], loaded.lake)
    observation = reconcile_topic(loaded.engine, TOPIC_SPECS[OBSERVATION_TOPIC], loaded.lake)
    assert forecast.bronze_distinct_events == 2 + 2  # live per location + backfill per location
    assert observation.bronze_distinct_events == 1 + 4  # live report + 2 stations x 2 IEM reports


@pytest.mark.integration
def test_silver_rebuilt_from_bronze_is_identical_to_the_original(loaded: Loaded) -> None:
    before = _snapshot(loaded.engine)
    assert before  # not vacuous

    for topic in (FORECAST_TOPIC, OBSERVATION_TOPIC):
        result = replay_from_bronze(topic, loaded.engine, loaded.lake, truncate=True)
        assert result.poison == 1  # the poison message is skipped, not fatal

    assert _snapshot(loaded.engine) == before  # every row, value, flag, and lineage id
    for spec in TOPIC_SPECS.values():
        assert reconcile_topic(loaded.engine, spec, loaded.lake).matched


@pytest.mark.integration
def test_reconciliation_detects_missing_and_unexplained_rows(loaded: Loaded) -> None:
    engine = loaded.engine
    forecast_spec = TOPIC_SPECS[FORECAST_TOPIC]

    with engine.begin() as conn:  # lose a row
        conn.execute(
            text(
                "DELETE FROM silver.forecast WHERE "
                "(model, location_id, init_time, valid_time, variable) = "
                "(SELECT model, location_id, init_time, valid_time, variable "
                "FROM silver.forecast LIMIT 1)"
            )
        )
    lost = reconcile_topic(engine, forecast_spec, loaded.lake)
    assert (lost.matched, lost.missing_from_silver, lost.extra_in_silver) == (False, 1, 0)

    # re-applying bronze without truncating repairs it (idempotent upserts)
    replay_from_bronze(FORECAST_TOPIC, engine, loaded.lake)
    assert reconcile_topic(engine, forecast_spec, loaded.lake).matched

    with engine.begin() as conn:  # a row bronze cannot explain
        conn.execute(
            text(
                "INSERT INTO silver.forecast (model, location_id, init_time, valid_time, "
                "variable, value, lead_hours, ingestion_mode, source_event_id) VALUES "
                "('phantom', 'nowhere', now(), now(), 'temperature_2m', 1.0, 0, 'live', 'x')"
            )
        )
    extra = reconcile_topic(engine, forecast_spec, loaded.lake)
    assert (extra.matched, extra.missing_from_silver, extra.extra_in_silver) == (False, 0, 1)

    # a truncating rebuild would DELETE that row, and it is indistinguishable from
    # real data that only exists in silver (e.g. the bronze sink was down) - so it
    # must refuse, and must do so before touching anything
    with pytest.raises(UnsafeRebuildError, match="1 rows"):
        replay_from_bronze(FORECAST_TOPIC, engine, loaded.lake, truncate=True)
    assert _scalar(engine, "SELECT count(*) FROM silver.forecast WHERE model = 'phantom'") == 1
    assert _scalar(engine, "SELECT count(*) FROM silver.forecast") > 1  # table not emptied

    # --force is the deliberate acceptance of that loss - and leaves the module's data consistent
    replay_from_bronze(FORECAST_TOPIC, engine, loaded.lake, truncate=True, force=True)
    assert reconcile_topic(engine, forecast_spec, loaded.lake).matched


@pytest.mark.integration
def test_trace_finds_real_events_in_bronze_and_silver(loaded: Loaded) -> None:
    from nimbus.jobs.trace import format_trace, sample_event_id, trace_event

    for topic, table in (
        (FORECAST_TOPIC, "silver.forecast"),
        (OBSERVATION_TOPIC, "silver.observation"),
    ):
        event_id = sample_event_id(topic, loaded.lake)
        assert event_id is not None

        trace = trace_event(loaded.engine, event_id, loaded.lake)

        assert trace.bronze and trace.bronze[0].topic == topic
        assert trace.silver_table == table
        assert trace.silver_expected > 0
        # every row the event yields is in silver, whichever event wrote it last
        assert sum(trace.silver_by_event.values()) == trace.silver_expected
        assert event_id in format_trace(trace)

    unknown = trace_event(loaded.engine, "no-such-event", loaded.lake)
    assert unknown.bronze == []
    assert "NOT FOUND" in format_trace(unknown)


@pytest.mark.integration
def test_reconciliation_compares_only_the_retention_window(loaded: Loaded) -> None:
    from datetime import UTC, datetime

    spec = TOPIC_SPECS[FORECAST_TOPIC]

    everything = reconcile_topic(loaded.engine, spec, loaded.lake)
    assert everything.matched and everything.silver_rows > 0

    # a window that starts after all the data: both sides are empty, so still a match
    future = datetime(2100, 1, 1, tzinfo=UTC)
    windowed = reconcile_topic(loaded.engine, spec, loaded.lake, valid_from=future)
    assert (windowed.expected_silver_rows, windowed.silver_rows) == (0, 0)
    assert windowed.matched
