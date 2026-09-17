"""Per-event-type payload schemas (brief section 6). Schema evolution: an
additive, backward-compatible payload change (a new optional field) doesn't
need a version bump; a breaking change to an existing field's meaning or
removal bumps `EventEnvelope.schema_version`, and consumers branch on it."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ForecastRawPayload(BaseModel):
    """One location's raw Open-Meteo response for one (model, run), tagged
    with the identifiers the API response itself doesn't carry. Mirrors the
    API response with minimal changes (brief section 6) - `api_response` is
    stored close to verbatim; unit conversion and tidying happen in
    `nimbus.transform.forecast`, not here."""

    model: str
    location_id: str
    run: datetime
    api_response: dict[str, Any]


class DlqRecord(BaseModel):
    """weather.dlq.v1 payload (brief section 6): the original payload, why it
    failed, where it came from, and when."""

    original_payload: str
    error_type: str
    error_message: str
    source_topic: str
    source_partition: int
    source_offset: int
    failed_at: datetime
