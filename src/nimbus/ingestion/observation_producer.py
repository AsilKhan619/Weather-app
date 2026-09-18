"""Live observation producer: aviationweather.gov METAR Data API ->
weather.observation.raw.v1 (brief sections 5-6, Phase 2). One poll cycle
fetches every configured station in a single batched request and produces
one idempotent event per report actually returned."""

import logging
import time
from datetime import UTC, datetime

import httpx
from confluent_kafka import Producer
from sqlalchemy.engine import Engine

from nimbus.common.config import Location, load_locations
from nimbus.common.db import make_engine, record_ingestion_run
from nimbus.common.events import EventEnvelope, compute_event_id
from nimbus.common.kafka import make_producer, produce_json
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import ObservationRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.shutdown import GracefulShutdown
from nimbus.ingestion.aviationweather import fetch_current_metars

logger = logging.getLogger(__name__)

TOPIC = "weather.observation.raw.v1"
POLL_INTERVAL_SECONDS = 300


def produce_one_poll_cycle(
    client: httpx.Client, producer: Producer, engine: Engine, locations: list[Location]
) -> int:
    """One pass: fetch every station's current report and produce one event
    each. A station with no current report simply doesn't appear in the
    response - that's normal (reporting interval, temporary outage), not an
    error, but it does count against `failed` for visibility."""
    started_at = datetime.now(UTC)
    station_ids = [loc.station for loc in locations]

    try:
        reports = fetch_current_metars(client, station_ids)
    except Exception:
        logger.exception("METAR fetch failed")
        record_ingestion_run(engine, "observation_producer", "live", started_at, 0, len(locations))
        return 0

    produced = 0
    seen_stations: set[str] = set()
    for report in reports:
        station = report.get("icaoId")
        obs_time = report.get("obsTime")
        if not station or obs_time is None:
            continue  # can't form a natural key without both
        seen_stations.add(station)

        observed_at = datetime.fromtimestamp(obs_time, tz=UTC)
        raw_text = str(report.get("rawOb") or "")
        payload = ObservationRawPayload(
            station=station, observed_at=observed_at, api_response=report
        )
        # Include raw_text in the key (brief section 6): a correction (COR)
        # to the same station/time is different text, so it gets its own id.
        event_id = compute_event_id(station, observed_at.isoformat(), raw_text)
        envelope = EventEnvelope[ObservationRawPayload](
            event_id=event_id,
            source="observation_producer",
            event_type="observation.raw",
            produced_at=datetime.now(UTC),
            ingestion_mode="live",
            payload=payload,
        )
        produce_json(producer, TOPIC, station, envelope.model_dump(mode="json"))
        logger.info("produced observation event", extra={"event_id": event_id, "station": station})
        produced += 1

    failed = len(set(station_ids) - seen_stations)
    producer.flush(10)
    record_ingestion_run(engine, "observation_producer", "live", started_at, produced, failed)
    return produced


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    locations = load_locations()
    engine = make_engine(settings)
    producer = make_producer(settings)
    shutdown = GracefulShutdown()

    with httpx.Client(timeout=30.0) as client:
        while not shutdown.should_stop:
            try:
                produce_one_poll_cycle(client, producer, engine, locations)
            except Exception:
                logger.exception("poll cycle failed")
            for _ in range(POLL_INTERVAL_SECONDS):
                if shutdown.should_stop:
                    break
                time.sleep(1)

    producer.flush(10)
    logger.info("observation producer stopped cleanly")


if __name__ == "__main__":
    main()
