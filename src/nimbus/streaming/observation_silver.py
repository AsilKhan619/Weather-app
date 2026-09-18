"""Silver consumer for observations (brief sections 6-8): validate ->
transform -> idempotent upsert. A corrected (COR) report for the same
(station, observed_at, variable) simply overwrites the earlier row - the
natural key never includes raw_text, so "keep the latest version of
corrected reports" falls out of the upsert for free."""

import logging
from collections.abc import Callable, Sequence

import pandas as pd
from confluent_kafka import Producer
from pydantic import ValidationError
from sqlalchemy.engine import Engine

from nimbus.common.db import chunked_upsert, dedupe_on_key, make_engine
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer, send_to_dlq
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import ObservationRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.tables import observation_table
from nimbus.streaming.cli import parse_drain_flag
from nimbus.streaming.microbatch import run_microbatch_loop
from nimbus.transform.observation import explode_observation_payload

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "weather.observation.raw.v1"
DLQ_TOPIC = "weather.dlq.v1"
CONSUMER_GROUP = "silver-observation"

_STRING_COLUMNS = ["station", "variable", "ingestion_mode", "source_event_id"]
_CONFLICT_COLUMNS = ["station", "observed_at", "variable"]
_UPDATE_COLUMNS = ["value", "raw_text", "is_corrected", "ingestion_mode", "source_event_id"]


def upsert_observation_rows(engine: Engine, rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    # A correction outranks the report it corrects even if both land in one batch.
    rows = dedupe_on_key(rows.sort_values("is_corrected", kind="stable"), _CONFLICT_COLUMNS)
    records = rows.astype(dict.fromkeys(_STRING_COLUMNS, "string")).to_dict("records")
    chunked_upsert(engine, observation_table, _CONFLICT_COLUMNS, _UPDATE_COLUMNS, records)


PoisonHandler = Callable[[KafkaMessageLike, Exception], None]


def message_to_frame(raw_value: bytes) -> pd.DataFrame:
    """Validate one raw observation message and explode it (shared by the live
    consumer, replay, and reconciliation - one definition of "what silver
    should contain for this message")."""
    envelope = EventEnvelope[ObservationRawPayload].model_validate_json(raw_value)
    frame = explode_observation_payload(envelope.payload, envelope.ingestion_mode)
    frame["source_event_id"] = envelope.event_id
    return frame


def load_messages(
    messages: Sequence[KafkaMessageLike], engine: Engine, on_poison: PoisonHandler
) -> int:
    """Validate -> transform -> upsert a batch; returns how many messages loaded.
    Kafka-independent so a replay from the bronze lake reuses it unchanged."""
    frames: list[pd.DataFrame] = []
    for msg in messages:
        try:
            raw_value = msg.value()
            if raw_value is None:
                raise ValueError("message has no value")
            frames.append(message_to_frame(raw_value))
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            on_poison(msg, exc)

    if frames:
        upsert_observation_rows(engine, pd.concat(frames, ignore_index=True))
    return len(frames)


def process_batch(
    messages: Sequence[KafkaMessageLike], engine: Engine, dlq_producer: Producer
) -> None:
    load_messages(messages, engine, lambda m, e: send_to_dlq(dlq_producer, m, e, SOURCE_TOPIC))
    dlq_producer.flush(10)


def main() -> None:
    drain = parse_drain_flag("Silver consumer: weather.observation.raw.v1 -> silver.observation")
    settings = get_settings()
    configure_logging(settings.log_level)
    consumer = make_consumer(settings, group_id=CONSUMER_GROUP)
    dlq_producer = make_producer(settings)
    engine = make_engine(settings)

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        process_batch(messages, engine, dlq_producer)

    run_microbatch_loop(consumer, [SOURCE_TOPIC], handle_batch, drain=drain)


if __name__ == "__main__":
    main()
