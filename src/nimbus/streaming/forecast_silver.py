"""Silver consumer for forecasts (brief sections 6-8): validate -> transform
-> idempotent upsert. A message that fails validation or transformation goes
to weather.dlq.v1 with the reason and never blocks the partition - the rest
of the batch still gets written."""

import logging
from collections.abc import Callable, Sequence

import pandas as pd
from confluent_kafka import Producer
from pydantic import BaseModel, ValidationError
from sqlalchemy.engine import Engine

from nimbus.common.db import chunked_upsert, dedupe_on_key, make_engine
from nimbus.common.events import FORECAST_BACKFILL_EVENT_TYPE, EventEnvelope
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer, send_to_dlq
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import ForecastBackfillRawPayload, ForecastRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.tables import forecast_table
from nimbus.streaming.cli import parse_drain_flag
from nimbus.streaming.microbatch import run_microbatch_loop
from nimbus.transform.forecast import explode_forecast_payload, explode_previous_runs_payload

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "weather.forecast.raw.v1"
DLQ_TOPIC = "weather.dlq.v1"
CONSUMER_GROUP = "silver-forecast"

_STRING_COLUMNS = ["model", "location_id", "variable", "ingestion_mode", "source_event_id"]
_CONFLICT_COLUMNS = ["model", "location_id", "init_time", "valid_time", "variable"]
_UPDATE_COLUMNS = ["value", "lead_hours", "ingestion_mode", "source_event_id"]


class _EventTypeProbe(BaseModel):
    event_type: str


def message_to_frame(raw_value: bytes) -> pd.DataFrame:
    """Validate one raw message and explode it. Live runs and Previous-Runs
    backfill chunks share this topic and table; `event_type` picks the transform
    (both emit identical columns)."""
    event_type = _EventTypeProbe.model_validate_json(raw_value).event_type
    if event_type == FORECAST_BACKFILL_EVENT_TYPE:
        backfill = EventEnvelope[ForecastBackfillRawPayload].model_validate_json(raw_value)
        frame = explode_previous_runs_payload(backfill.payload)
        frame["source_event_id"] = backfill.event_id
        return frame
    live = EventEnvelope[ForecastRawPayload].model_validate_json(raw_value)
    frame = explode_forecast_payload(live.payload, live.ingestion_mode)
    frame["source_event_id"] = live.event_id
    return frame


def upsert_forecast_rows(engine: Engine, rows: pd.DataFrame) -> None:
    """Insert-or-update on the natural key (brief section 7) - re-processing
    the same event always produces the same rows, so this is safe to run
    twice with identical input."""
    if rows.empty:
        return
    rows = dedupe_on_key(rows, _CONFLICT_COLUMNS)
    records = rows.astype(dict.fromkeys(_STRING_COLUMNS, "string")).to_dict("records")
    chunked_upsert(
        engine,
        forecast_table,
        _CONFLICT_COLUMNS,
        _UPDATE_COLUMNS,
        records,
        only_if_changed=True,
        touch_columns=("updated_at",),
    )


PoisonHandler = Callable[[KafkaMessageLike, Exception], None]


def load_messages(
    messages: Sequence[KafkaMessageLike], engine: Engine, on_poison: PoisonHandler
) -> int:
    """Validate -> transform -> upsert a batch; returns how many messages loaded.
    Independent of Kafka (works on anything message-shaped), so the same code
    serves the live consumer and a replay from the bronze lake. A message that
    fails validation or transformation goes to `on_poison` and never blocks the
    rest of the batch."""
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
        upsert_forecast_rows(engine, pd.concat(frames, ignore_index=True))
    return len(frames)


def process_batch(
    messages: Sequence[KafkaMessageLike], engine: Engine, dlq_producer: Producer
) -> None:
    load_messages(messages, engine, lambda m, e: send_to_dlq(dlq_producer, m, e, SOURCE_TOPIC))
    dlq_producer.flush(10)


def main() -> None:
    drain = parse_drain_flag("Silver consumer: weather.forecast.raw.v1 -> silver.forecast")
    settings = get_settings()
    configure_logging(settings.log_level)
    consumer = make_consumer(settings, group_id=CONSUMER_GROUP)
    dlq_producer = make_producer(settings)
    engine = make_engine(settings)

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        process_batch(messages, engine, dlq_producer)

    # Backfill events carry thousands of rows each, so keep batches small enough
    # that one batch's frame stays comfortably in memory.
    run_microbatch_loop(consumer, [SOURCE_TOPIC], handle_batch, max_batch_size=50, drain=drain)


if __name__ == "__main__":
    main()
