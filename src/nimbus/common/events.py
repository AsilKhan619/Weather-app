"""The event envelope every Kafka message uses (brief section 6). Per-event-type
payload schemas are added in Phase 1 (forecast) and Phase 2 (observation)."""

import hashlib
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

IngestionMode = Literal["live", "backfill"]

FORECAST_EVENT_TYPE = "forecast.raw"
FORECAST_BACKFILL_EVENT_TYPE = "forecast.backfill.raw"
OBSERVATION_EVENT_TYPE = "observation.raw"


class EventEnvelope[PayloadT](BaseModel):
    event_id: str
    schema_version: int = 1
    source: str
    event_type: str
    produced_at: datetime
    ingestion_mode: IngestionMode
    payload: PayloadT


def compute_event_id(*natural_key_parts: str) -> str:
    """Deterministic id from a message's natural key, so re-ingesting identical
    data produces the same event_id (idempotent upserts, brief section 6)."""
    digest = hashlib.sha256("|".join(natural_key_parts).encode("utf-8"))
    return digest.hexdigest()
