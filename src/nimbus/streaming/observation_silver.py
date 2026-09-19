"""Silver consumer for observations (brief sections 6-8): validate ->
transform -> idempotent upsert. A corrected (COR) report for the same
(station, observed_at, variable) simply overwrites the earlier row - the
natural key never includes raw_text, so "keep the latest version of
corrected reports" falls out of the upsert for free."""

import logging
from collections.abc import Callable, Sequence
from typing import Any

import pandas as pd
from confluent_kafka import Producer
from pydantic import ValidationError
from sqlalchemy import ColumnElement, not_, or_
from sqlalchemy.engine import Engine

from nimbus.common.db import chunked_upsert, dedupe_on_key, make_engine
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer, send_to_dlq
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import ObservationRawPayload
from nimbus.common.settings import get_settings
from nimbus.common.tables import observation_table
from nimbus.quality.gate import gate_frame
from nimbus.quality.runner import QualityError, persist_results
from nimbus.quality.schemas import silver_observation_gate
from nimbus.streaming.cli import parse_drain_flag
from nimbus.streaming.microbatch import run_microbatch_loop
from nimbus.transform.observation import observation_frame, observation_rows

logger = logging.getLogger(__name__)

SOURCE_TOPIC = "weather.observation.raw.v1"
DLQ_TOPIC = "weather.dlq.v1"
CONSUMER_GROUP = "silver-observation"

_STRING_COLUMNS = ["station", "variable", "ingestion_mode", "source_event_id"]
_CONFLICT_COLUMNS = ["station", "observed_at", "variable"]
_UPDATE_COLUMNS = ["value", "raw_text", "is_corrected", "ingestion_mode", "source_event_id"]


def _may_overwrite(excluded: Any) -> ColumnElement[bool]:
    """A stored correction (COR) is only replaced by another correction, never by
    an uncorrected report - even one arriving in a later batch (an overlapping
    backfill window, a live poll that still lists the original). Combined with
    the in-batch sort below, the outcome no longer depends on arrival order."""
    return or_(excluded.is_corrected, not_(observation_table.c.is_corrected))


def upsert_observation_rows(engine: Engine, rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    # A correction outranks the report it corrects even if both land in one batch.
    rows = dedupe_on_key(rows.sort_values("is_corrected", kind="stable"), _CONFLICT_COLUMNS)
    records = rows.astype(dict.fromkeys(_STRING_COLUMNS, "string")).to_dict("records")
    chunked_upsert(
        engine,
        observation_table,
        _CONFLICT_COLUMNS,
        _UPDATE_COLUMNS,
        records,
        update_where=_may_overwrite,
        only_if_changed=True,
        touch_columns=("updated_at",),
    )


PoisonHandler = Callable[[KafkaMessageLike, Exception], None]


def message_to_rows(raw_value: bytes) -> list[dict[str, Any]]:
    """Validate one raw observation message into plain row dicts, with the
    lineage `source_event_id` (shared by the live consumer, replay, and
    reconciliation - one definition of "what silver should contain for this
    message"). Deliberately no pandas: this runs once per message, and a
    DataFrame per 4-row message dominated the whole pipeline (~9 ms each,
    ADR 0004). Callers batch many messages' rows into one frame."""
    envelope = EventEnvelope[ObservationRawPayload].model_validate_json(raw_value)
    rows = observation_rows(envelope.payload, envelope.ingestion_mode)
    for row in rows:
        row["source_event_id"] = envelope.event_id
    return rows


def message_to_frame(raw_value: bytes) -> pd.DataFrame:
    """Single-message frame. Bulk paths use `message_to_rows` and build one
    frame per batch instead."""
    return observation_frame(message_to_rows(raw_value))


def load_messages(
    messages: Sequence[KafkaMessageLike], engine: Engine, on_poison: PoisonHandler
) -> int:
    """Validate -> transform -> upsert a batch; returns how many messages loaded.
    Kafka-independent so a replay from the bronze lake reuses it unchanged."""
    rows: list[dict[str, Any]] = []
    by_event: dict[str, KafkaMessageLike] = {}
    for msg in messages:
        try:
            raw_value = msg.value()
            if raw_value is None:
                raise ValueError("message has no value")
            message_rows = message_to_rows(raw_value)
            rows.extend(message_rows)
            by_event[str(message_rows[0]["source_event_id"])] = msg
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            on_poison(msg, exc)

    if not rows:
        return 0
    gate = gate_frame(observation_frame(rows), silver_observation_gate())
    for event_id in sorted(gate.blocked_events):
        on_poison(by_event[event_id], QualityError("failed a blocking quality check"))
    if gate.failed:
        persist_results(engine, gate.failed, context="silver-observation batch", only_failures=True)
    upsert_observation_rows(engine, gate.clean)
    return len(by_event) - len(gate.blocked_events)


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
