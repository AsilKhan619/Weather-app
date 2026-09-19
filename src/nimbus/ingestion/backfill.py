"""Historical backfill through the same topics as live ingestion (brief
sections 4-5, ADR 0004): forecasts from Open-Meteo's Previous Runs API,
observations from the IEM ASOS archive. The silver consumers don't know or care
that an event is historical - `ingestion_mode='backfill'` is only a flag.

Every event id is deterministic, so re-running an overlapping window (a resume
after a rate-limit abort, or a retry) is a no-op at the database."""

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import httpx
from confluent_kafka import Producer

from nimbus.common.config import Location, ModelsConfig
from nimbus.common.events import (
    FORECAST_BACKFILL_EVENT_TYPE,
    EventEnvelope,
    compute_event_id,
)
from nimbus.common.kafka import produce_json
from nimbus.common.schemas import ForecastBackfillRawPayload
from nimbus.ingestion.iem_asos import fetch_station_history, parse_iem_csv
from nimbus.ingestion.observation_producer import TOPIC as OBSERVATION_TOPIC
from nimbus.ingestion.observation_producer import build_observation_event
from nimbus.ingestion.open_meteo import fetch_previous_runs

logger = logging.getLogger(__name__)

FORECAST_TOPIC = "weather.forecast.raw.v1"

# Open-Meteo bills by data volume (>10 variables or >14 days per location count
# as multiple calls, fractionally) against 600/minute and 10,000/day.
#
# Request SIZE is a second, separate limit, found by the first real run (ADR
# 0004): a 25-location, 28-column, 7-day request returns ~700 KB and the server
# truncates the body mid-JSON (HTTP 200, invalid JSON) for the larger models.
# Splitting locations into batches of 10 keeps each response ~250 KB. Weighted
# cost is unchanged by batching (it scales with locations x variables x days);
# 10 locations x 28 columns x 7 days is ~14 calls, so ~2s between requests keeps
# well under the per-minute cap (~420 calls/min).
DEFAULT_FORECAST_CHUNK_DAYS = 7
DEFAULT_FORECAST_LOCATION_BATCH = 10
DEFAULT_FORECAST_THROTTLE_SECONDS = 2.0
DEFAULT_OBSERVATION_CHUNK_DAYS = 90
DEFAULT_OBSERVATION_THROTTLE_SECONDS = 0.5


@dataclass
class BackfillResult:
    produced: int = 0
    failed: int = 0
    # One human-readable line per failed request (what and from when), so a
    # partial run says exactly what to redo rather than just a count.
    failures: list[str] = field(default_factory=list)
    # Set when the provider rate-limited us: re-run with --start-date on this
    # date to resume (everything before it completed, and re-work is idempotent).
    aborted_at: date | None = None


def chunk_dates(start: date, end: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Inclusive (first, last) windows covering start..end."""
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    cursor = start
    while cursor <= end:
        last = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, last
        cursor = last + timedelta(days=1)


def location_batches[T](items: list[T], size: int) -> Iterator[list[T]]:
    if size < 1:
        raise ValueError("batch size must be >= 1")
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _is_rate_limited(exc: Exception) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429


def backfill_forecasts(
    client: httpx.Client,
    producer: Producer,
    locations: list[Location],
    models: ModelsConfig,
    start: date,
    end: date,
    *,
    chunk_days: int = DEFAULT_FORECAST_CHUNK_DAYS,
    location_batch_size: int = DEFAULT_FORECAST_LOCATION_BATCH,
    throttle_seconds: float = DEFAULT_FORECAST_THROTTLE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> BackfillResult:
    """Chunk-outer, then model, then location batch, so `aborted_at` is a clean
    resume point: every model and location finished every chunk before it. A
    failed request loses only its own (chunk, model, location batch); the rest
    still land."""
    result = BackfillResult()
    lead_key = ",".join(str(n) for n in models.backfill_lead_days)

    for chunk_start, chunk_end in chunk_dates(start, end, chunk_days):
        for model in models.models:
            for batch in location_batches(locations, location_batch_size):
                try:
                    responses = fetch_previous_runs(
                        client,
                        model.id,
                        [(loc.latitude, loc.longitude) for loc in batch],
                        models.variables,
                        models.backfill_lead_days,
                        chunk_start,
                        chunk_end,
                    )
                    if len(responses) != len(batch):
                        raise ValueError(f"expected {len(batch)} responses, got {len(responses)}")
                except Exception as exc:
                    if _is_rate_limited(exc):
                        logger.error(
                            "rate limited; stopping", extra={"resume_from": str(chunk_start)}
                        )
                        result.aborted_at = chunk_start
                        return result
                    logger.exception(
                        "forecast backfill request failed",
                        extra={
                            "model": model.id,
                            "chunk_start": str(chunk_start),
                            "locations": [loc.id for loc in batch],
                        },
                    )
                    result.failed += 1
                    result.failures.append(
                        f"forecasts {model.id} {chunk_start} ({batch[0].id}..{batch[-1].id})"
                    )
                    sleep(throttle_seconds)
                    continue

                for location, api_response in zip(batch, responses, strict=True):
                    event_id = compute_event_id(
                        FORECAST_BACKFILL_EVENT_TYPE,
                        model.id,
                        location.id,
                        chunk_start.isoformat(),
                        chunk_end.isoformat(),
                        lead_key,
                    )
                    envelope = EventEnvelope[ForecastBackfillRawPayload](
                        event_id=event_id,
                        source="forecast_backfill",
                        event_type=FORECAST_BACKFILL_EVENT_TYPE,
                        produced_at=datetime.now(UTC),
                        ingestion_mode="backfill",
                        payload=ForecastBackfillRawPayload(
                            model=model.id,
                            location_id=location.id,
                            start_date=chunk_start,
                            end_date=chunk_end,
                            api_response=api_response,
                        ),
                    )
                    produce_json(
                        producer, FORECAST_TOPIC, location.id, envelope.model_dump(mode="json")
                    )
                    result.produced += 1
                logger.info(
                    "backfilled forecast chunk",
                    extra={
                        "model": model.id,
                        "chunk_start": str(chunk_start),
                        "locations": len(batch),
                    },
                )
                sleep(throttle_seconds)

    return result


def backfill_observations(
    client: httpx.Client,
    producer: Producer,
    locations: list[Location],
    start: date,
    end: date,
    *,
    chunk_days: int = DEFAULT_OBSERVATION_CHUNK_DAYS,
    throttle_seconds: float = DEFAULT_OBSERVATION_THROTTLE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> BackfillResult:
    """One request per (station, chunk). IEM is a free academic service, so be
    polite: sequential requests with a pause, never parallel."""
    result = BackfillResult()

    for location in locations:
        for chunk_start, chunk_end in chunk_dates(start, end, chunk_days):
            try:
                csv_text = fetch_station_history(
                    client, location.station, chunk_start, chunk_end + timedelta(days=1)
                )
                reports = parse_iem_csv(csv_text, location.station)
            except Exception:
                logger.exception(
                    "observation backfill chunk failed",
                    extra={"station": location.station, "chunk_start": str(chunk_start)},
                )
                result.failed += 1
                result.failures.append(f"observations {location.station} {chunk_start}")
                sleep(throttle_seconds)
                continue

            for report in reports:
                built = build_observation_event(
                    report, ingestion_mode="backfill", source="observation_backfill"
                )
                if built is None:
                    continue
                station, _event_id, envelope = built
                produce_json(producer, OBSERVATION_TOPIC, station, envelope)
                result.produced += 1
            logger.info(
                "backfilled observation chunk",
                extra={"station": location.station, "reports": len(reports)},
            )
            sleep(throttle_seconds)

    return result
