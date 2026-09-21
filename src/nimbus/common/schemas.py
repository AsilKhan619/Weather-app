"""Per-event-type payload schemas (brief section 6). Schema evolution: an
additive, backward-compatible payload change (a new optional field) doesn't
need a version bump; a breaking change to an existing field's meaning or
removal bumps `EventEnvelope.schema_version`, and consumers branch on it."""

from datetime import date, datetime
from typing import Any, Literal

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


class ForecastBackfillRawPayload(BaseModel):
    """One location's raw Open-Meteo Previous Runs API response for one model
    over a date range. The API gives fixed lead-time offsets (`*_previous_dayN`)
    rather than exact model run times, so there is no `run` here - the silver
    transform derives init_time from valid_time minus the lead offset (ADR 0004)."""

    model: str
    location_id: str
    start_date: date
    end_date: date
    api_response: dict[str, Any]


class ObservationRawPayload(BaseModel):
    """One station's raw aviationweather.gov METAR report, tagged with the
    identifiers needed downstream. Mirrors the API response with minimal
    changes (brief section 6) - `api_response` is the JSON object close to
    verbatim; unit conversion and METAR-text parsing (COR flag) happen in
    `nimbus.transform.observation`, not here."""

    station: str
    observed_at: datetime
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


AlertRule = Literal["run_change", "model_spread", "observation_miss"]
AlertSeverity = Literal["warning", "critical"]


class AlertPayload(BaseModel):
    """weather.alert.v1 payload (brief section 9). `alert_id` is deterministic from
    (rule, subject, event time, variable), so re-detecting the same anomaly - after a
    restart or a replay - yields the same alert rather than a new one."""

    alert_id: str
    rule: AlertRule
    severity: AlertSeverity
    location_id: str
    variable: str
    model: str | None = None  # run_change: which model changed; None for cross-model rules
    station: str | None = None  # observation_miss
    event_time: datetime  # the model run (run_change, model_spread) or the observation time
    metric: float  # the measured value (same unit as the variable)
    threshold: float
    details: dict[str, Any]
    triggered_by_event_id: str
    detected_at: datetime
