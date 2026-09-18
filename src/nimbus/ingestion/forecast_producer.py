"""Live forecast producer: Open-Meteo Single Runs API -> weather.forecast.raw.v1
(brief sections 5-6, Phase 1). One poll cycle finds the latest available run
per model and produces one idempotent event per (model, location)."""

import logging
import time
from datetime import UTC, datetime

import httpx
from confluent_kafka import Producer
from sqlalchemy.engine import Engine

from nimbus.common.config import Location, ModelsConfig, load_locations, load_models_config
from nimbus.common.db import make_engine, record_ingestion_run
from nimbus.common.events import EventEnvelope, compute_event_id
from nimbus.common.kafka import make_producer, produce_json
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import ForecastRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.shutdown import GracefulShutdown
from nimbus.ingestion.open_meteo import fetch_forecast_run, find_latest_available_run

logger = logging.getLogger(__name__)

TOPIC = "weather.forecast.raw.v1"
POLL_INTERVAL_SECONDS = 300


def produce_one_poll_cycle(
    client: httpx.Client,
    producer: Producer,
    engine: Engine,
    locations: list[Location],
    models: ModelsConfig,
) -> int:
    """One pass over every configured model: find its latest available run
    and produce one event per location. Returns messages produced."""
    started_at = datetime.now(UTC)
    produced = 0
    failed = 0

    for model in models.models:
        probe = locations[0]
        try:
            run = find_latest_available_run(
                client,
                model.id,
                probe.latitude,
                probe.longitude,
                cadence_hours=models.run_cadence_hours,
                lookback_steps=models.run_lookback_steps,
            )
            responses = fetch_forecast_run(
                client,
                model.id,
                run,
                [(loc.latitude, loc.longitude) for loc in locations],
                models.variables,
                models.forecast_days,
            )
        except Exception:
            logger.exception("forecast poll failed for model", extra={"model": model.id})
            failed += 1
            continue

        for location, api_response in zip(locations, responses, strict=True):
            payload = ForecastRawPayload(
                model=model.id,
                location_id=location.id,
                run=run,
                api_response=api_response,
            )
            event_id = compute_event_id(model.id, location.id, run.isoformat())
            envelope = EventEnvelope[ForecastRawPayload](
                event_id=event_id,
                source="forecast_producer",
                event_type="forecast.raw",
                produced_at=datetime.now(UTC),
                ingestion_mode="live",
                payload=payload,
            )
            produce_json(producer, TOPIC, location.id, envelope.model_dump(mode="json"))
            logger.info(
                "produced forecast event",
                extra={"event_id": event_id, "model": model.id, "location_id": location.id},
            )
            produced += 1

    producer.flush(10)
    record_ingestion_run(engine, "forecast_producer", "live", started_at, produced, failed)
    return produced


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    locations = load_locations()
    models = load_models_config()
    engine = make_engine(settings)
    producer = make_producer(settings)
    shutdown = GracefulShutdown()

    with httpx.Client(timeout=30.0) as client:
        while not shutdown.should_stop:
            try:
                produce_one_poll_cycle(client, producer, engine, locations, models)
            except Exception:
                logger.exception("poll cycle failed")
            for _ in range(POLL_INTERVAL_SECONDS):
                if shutdown.should_stop:
                    break
                time.sleep(1)

    producer.flush(10)
    logger.info("forecast producer stopped cleanly")


if __name__ == "__main__":
    main()
