"""Briefings when alerts arrive (brief section 10): consumes weather.alert.v1 and briefs each
alerted location once per batch, with the alert in its fact sheet.

The fact sheet is taken as of the top of the current hour - or as of the alert itself if
that is later - so a burst of alerts for one location (the three models of one cycle, say)
lands on one fact sheet and one cached briefing rather than one paid call each, and the
alert that triggered the briefing is always in it. (Found by the phase review: an observation
alert at 14:51 used to be cut off by a 14:00 fact sheet, so the "alert" briefing was the
daily one.) With LLM_ENABLED=false the consumer still runs and commits: the skip is
logged per location, and the alerts themselves are untouched."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from confluent_kafka import Producer
from pydantic import ValidationError
from sqlalchemy import Engine

from nimbus.common.config import LLMConfig, Location, load_llm_config, load_locations
from nimbus.common.db import make_engine
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import AlertPayload
from nimbus.common.settings import get_settings
from nimbus.jobs.generate_briefings import make_briefing_client
from nimbus.llm.briefings import BriefingResult, generate_briefing, publish_pending
from nimbus.llm.client import BriefingClient
from nimbus.streaming.cli import parse_drain_flag
from nimbus.streaming.microbatch import run_microbatch_loop

logger = logging.getLogger(__name__)

ALERT_TOPIC = "weather.alert.v1"
CONSUMER_GROUP = "briefing-on-alert"


def alerts_by_location(messages: Sequence[KafkaMessageLike]) -> dict[str, AlertPayload]:
    """The latest alert per location in the batch (by event time, then detection time);
    unreadable messages are skipped."""
    latest: dict[str, AlertPayload] = {}
    for msg in messages:
        raw = msg.value()
        if raw is None:
            continue
        try:
            alert = EventEnvelope[AlertPayload].model_validate_json(raw).payload
        except ValidationError:
            logger.warning("skipped an unreadable alert event")
            continue
        current = latest.get(alert.location_id)
        if current is None or (alert.event_time, alert.detected_at) >= (
            current.event_time,
            current.detected_at,
        ):
            latest[alert.location_id] = alert
    return latest


def process_batch(
    messages: Sequence[KafkaMessageLike],
    engine: Engine,
    locations: dict[str, Location],
    *,
    client: BriefingClient | None,
    config: LLMConfig,
    producer: Producer | None,
    model: str,
    as_of: datetime | None = None,
) -> list[BriefingResult]:
    hour = as_of or datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    results = []
    for location_id, alert in sorted(alerts_by_location(messages).items()):
        location = locations.get(location_id)
        if location is None:
            continue
        results.append(
            generate_briefing(
                engine,
                location,
                max(hour, alert.event_time),
                client=client,
                config=config,
                producer=producer,
                trigger="alert",
                trigger_ref=alert.alert_id,
                model=model,
            )
        )
    return results


def main() -> None:
    drain = parse_drain_flag("Briefings on alerts: weather.alert.v1 -> weather.briefing.v1")
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)
    config = load_llm_config()
    client = make_briefing_client(settings)
    producer = make_producer(settings) if client is not None else None
    locations = {loc.id: loc for loc in load_locations()}
    if producer is not None:
        publish_pending(engine, producer)
    consumer = make_consumer(settings, group_id=CONSUMER_GROUP)

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        process_batch(
            messages,
            engine,
            locations,
            client=client,
            config=config,
            producer=producer,
            model=settings.nimbus_briefing_model,
        )

    run_microbatch_loop(consumer, [ALERT_TOPIC], handle_batch, drain=drain)


if __name__ == "__main__":
    main()
