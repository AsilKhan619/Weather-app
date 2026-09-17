"""Silver consumer for forecasts (brief sections 6-8): validate -> transform
-> idempotent upsert. A message that fails validation or transformation goes
to weather.dlq.v1 with the reason and never blocks the partition - the rest
of the batch still gets written."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

import pandas as pd
from confluent_kafka import Producer
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from nimbus.common.db import make_engine
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer, produce_json
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import DlqRecord, ForecastRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.tables import forecast_table
from nimbus.streaming.microbatch import run_microbatch_loop
from nimbus.transform.forecast import explode_forecast_payload

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "weather.forecast.raw.v1"
DLQ_TOPIC = "weather.dlq.v1"
CONSUMER_GROUP = "silver-forecast"

# Postgres caps a single statement at 65535 bound parameters. forecast_table
# has 9 columns, so 5000 rows/statement (45000 params) stays well clear of
# that limit even before accounting for the ON CONFLICT clause's own binds.
_UPSERT_CHUNK_SIZE = 5000


def _send_to_dlq(producer: Producer, msg: KafkaMessageLike, error: Exception) -> None:
    raw_value = msg.value() or b""
    record = DlqRecord(
        original_payload=raw_value.decode("utf-8", errors="replace"),
        error_type=type(error).__name__,
        error_message=str(error),
        source_topic=msg.topic() or SOURCE_TOPIC,
        source_partition=msg.partition() or 0,
        source_offset=msg.offset() or 0,
        failed_at=datetime.now(UTC),
    )
    produce_json(producer, DLQ_TOPIC, key=record.source_topic, value=record.model_dump(mode="json"))
    logger.warning(
        "routed message to DLQ",
        extra={"error_type": record.error_type, "error_message": record.error_message},
    )


def upsert_forecast_rows(engine: Engine, rows: pd.DataFrame) -> None:
    """Insert-or-update on the natural key (brief section 7) - re-processing
    the same event always produces the same rows, so this is safe to run
    twice with identical input."""
    if rows.empty:
        return
    records = rows.astype(
        {
            "model": "string",
            "location_id": "string",
            "variable": "string",
            "ingestion_mode": "string",
            "source_event_id": "string",
        }
    ).to_dict("records")

    with engine.begin() as conn:
        for start in range(0, len(records), _UPSERT_CHUNK_SIZE):
            chunk = records[start : start + _UPSERT_CHUNK_SIZE]
            stmt = pg_insert(forecast_table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["model", "location_id", "init_time", "valid_time", "variable"],
                set_={
                    "value": stmt.excluded.value,
                    "lead_hours": stmt.excluded.lead_hours,
                    "ingestion_mode": stmt.excluded.ingestion_mode,
                    "source_event_id": stmt.excluded.source_event_id,
                },
            )
            conn.execute(stmt)


def process_batch(
    messages: Sequence[KafkaMessageLike], engine: Engine, dlq_producer: Producer
) -> None:
    frames: list[pd.DataFrame] = []
    for msg in messages:
        try:
            raw_value = msg.value()
            if raw_value is None:
                raise ValueError("message has no value")
            envelope = EventEnvelope[ForecastRawPayload].model_validate_json(raw_value)
            df = explode_forecast_payload(envelope.payload, envelope.ingestion_mode)
            df["source_event_id"] = envelope.event_id
            frames.append(df)
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            _send_to_dlq(dlq_producer, msg, exc)

    if frames:
        upsert_forecast_rows(engine, pd.concat(frames, ignore_index=True))
    dlq_producer.flush(10)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    consumer = make_consumer(settings, group_id=CONSUMER_GROUP)
    dlq_producer = make_producer(settings)
    engine = make_engine(settings)

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        process_batch(messages, engine, dlq_producer)

    run_microbatch_loop(consumer, [SOURCE_TOPIC], handle_batch)


if __name__ == "__main__":
    main()
