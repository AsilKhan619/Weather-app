"""`python -m nimbus.jobs.backfill` - load historical forecasts and observations
through the same Kafka topics as live ingestion (brief sections 4-5, ADR 0004).

  --days N              the last N complete days (default 30; what `make demo` uses)
  --full                everything since 2024-01-01 (what `make backfill` uses)
  --start-date/--end-date   an explicit window - also how to resume after a
                        rate-limit abort (the job prints the date to resume from)
  --only forecasts|observations

Events are idempotent, so re-running an overlapping window is safe. This only
*produces*; run the consumers (`make drain`) to land the data in bronze/silver."""

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta

import httpx
from confluent_kafka import Producer
from sqlalchemy import Engine

from nimbus.common.config import Location, ModelsConfig, load_locations, load_models_config
from nimbus.common.db import make_engine, record_ingestion_run
from nimbus.common.kafka import make_producer
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.ingestion.backfill import (
    DEFAULT_FORECAST_CHUNK_DAYS,
    BackfillResult,
    backfill_forecasts,
    backfill_observations,
)

logger = logging.getLogger(__name__)

# Previous Runs data is archived from January 2024 for most models (ADR 0001).
FULL_HISTORY_START = date(2024, 1, 1)

# Open-Meteo's non-commercial cap is 10,000 weighted calls/day (ADR 0004).
DAILY_CALL_BUDGET = 10_000


def resolve_window(
    *, days: int, full: bool, start: date | None, end: date | None, today: date
) -> tuple[date, date]:
    """End defaults to yesterday: today's observations and lead-offset forecasts
    are still incomplete."""
    resolved_end = end or today - timedelta(days=1)
    if start is not None:
        resolved_start = start
    elif full:
        resolved_start = FULL_HISTORY_START
    else:
        resolved_start = resolved_end - timedelta(days=days - 1)
    if resolved_start > resolved_end:
        raise ValueError(f"start {resolved_start} is after end {resolved_end}")
    return resolved_start, resolved_end


def estimate_forecast_calls(
    models: ModelsConfig, n_locations: int, start: date, end: date
) -> float:
    """Open-Meteo weights a request by data volume: each 10 variables and each
    14 days per location counts as one call, fractionally."""
    n_columns = len(models.variables) * len(models.backfill_lead_days)
    days = (end - start).days + 1
    return n_locations * len(models.models) * (n_columns / 10) * (days / 14)


def _report(name: str, result: BackfillResult) -> None:
    print(f"{name}: produced {result.produced:,} events, {result.failed:,} failed chunks")
    if result.aborted_at is not None:
        print(f"  RATE LIMITED - resume with: --start-date {result.aborted_at.isoformat()}")


def run(
    client: httpx.Client,
    engine: Engine,
    producer: Producer,
    locations: list[Location],
    models: ModelsConfig,
    start: date,
    end: date,
    only: str | None,
) -> bool:
    """Returns True if everything completed cleanly."""
    ok = True

    if only in (None, "forecasts"):
        started = datetime.now(UTC)
        forecasts = backfill_forecasts(
            client, producer, locations, models, start, end, chunk_days=DEFAULT_FORECAST_CHUNK_DAYS
        )
        producer.flush(60)
        record_ingestion_run(
            engine, "forecast_backfill", "backfill", started, forecasts.produced, forecasts.failed
        )
        _report("forecasts", forecasts)
        ok &= forecasts.failed == 0 and forecasts.aborted_at is None

    if only in (None, "observations"):
        started = datetime.now(UTC)
        observations = backfill_observations(client, producer, locations, start, end)
        producer.flush(60)
        record_ingestion_run(
            engine,
            "observation_backfill",
            "backfill",
            started,
            observations.produced,
            observations.failed,
        )
        _report("observations", observations)
        ok &= observations.failed == 0

    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical forecasts and observations.")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--only", choices=["forecasts", "observations"])
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    locations = load_locations()
    models = load_models_config()
    start, end = resolve_window(
        days=args.days,
        full=args.full,
        start=args.start_date,
        end=args.end_date,
        today=datetime.now(UTC).date(),
    )
    print(f"backfilling {start} .. {end} for {len(locations)} locations")

    if args.only in (None, "forecasts"):
        estimate = estimate_forecast_calls(models, len(locations), start, end)
        print(
            f"estimated Open-Meteo cost: ~{estimate:,.0f} weighted calls "
            f"(daily cap {DAILY_CALL_BUDGET:,})"
        )
        if estimate > DAILY_CALL_BUDGET * 0.9:
            print(
                "  WARNING: over 90% of one day's budget. If it hits the limit the job stops "
                "and prints the date to resume from; re-run then with --start-date."
            )

    engine = make_engine(settings)
    producer = make_producer(settings)
    with httpx.Client(
        timeout=60.0,
        headers={"User-Agent": "nimbus-weather-platform (github.com/AsilKhan619/Weather-app)"},
    ) as client:
        ok = run(client, engine, producer, locations, models, start, end, args.only)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
