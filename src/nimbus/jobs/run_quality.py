"""`make quality`: the quality suite (brief section 8). Validates recently changed
silver rows and gold rows with the same pandera schemas the loads use, checks
freshness, records every result in ops.quality_results, and exits non-zero if a
blocking check failed.

Rows are selected by `updated_at` / `computed_at` (the change tracking the gold
build also uses), so a scheduled run costs in proportion to what changed; `--all`
checks everything, streaming each table in chunks."""

import argparse
import logging
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy import Connection, Engine, text

from nimbus.common.config import load_gold_config, load_variables
from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.quality.freshness import check_freshness
from nimbus.quality.runner import CheckResult, merge_results, persist_results, validate_frame
from nimbus.quality.schemas import (
    TableChecks,
    accuracy_checks,
    forecast_checks,
    observation_checks,
    verification_checks,
)

logger = logging.getLogger(__name__)

CHUNK_ROWS = 200_000

# Table and column lists are constants defined here, never user input.
_FORECAST_SELECT = (
    "model, location_id, init_time, valid_time, variable, value, lead_hours, "
    "ingestion_mode, source_event_id"
)
_OBSERVATION_SELECT = (
    "station, observed_at, variable, value, raw_text, is_corrected, ingestion_mode, source_event_id"
)
_VERIFICATION_SELECT = (
    "model, location_id, init_time, valid_time, variable, lead_hours, lead_day, "
    "forecast_value, observed_value, observed_at, obs_offset_seconds, error, ingestion_mode"
)
_ACCURACY_SELECT = "valid_date, location_id, model, variable, lead_day, n, bias, mae, rmse"


def _chunks(
    conn: Connection, table: str, columns: str, stamp: str, since: datetime | None
) -> Iterator[pd.DataFrame]:
    where = "" if since is None else f" WHERE {stamp} >= :since"
    query = text(f"SELECT {columns} FROM {table}{where}")
    params = {} if since is None else {"since": since}
    stream = conn.execution_options(stream_results=True)
    yield from pd.read_sql(query, stream, params=params, chunksize=CHUNK_ROWS)


_TIME_COLUMNS = ("init_time", "valid_time", "observed_at")
_FLOAT_COLUMNS = ("value", "forecast_value", "observed_value", "error", "bias", "mae", "rmse")


def _coerce_types(frame: pd.DataFrame) -> pd.DataFrame:
    """A chunk whose float column is entirely NULL arrives as object dtype; timestamps
    may arrive in the session's offset. Normalise both so the dtype checks judge the
    data, not how it happened to be fetched."""
    for column in _TIME_COLUMNS:
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True)
    for column in _FLOAT_COLUMNS:
        if column in frame:
            frame[column] = frame[column].astype("float64")
    return frame


def validate_table(
    engine: Engine,
    table: str,
    columns: str,
    stamp: str,
    checks: TableChecks,
    since: datetime | None,
) -> list[CheckResult]:
    """Validate one table chunk by chunk. Uniqueness is asserted within a chunk; across
    chunks the primary key guarantees it."""
    results: list[CheckResult] = []
    with engine.connect() as conn:
        for chunk in _chunks(conn, table, columns, stamp, since):
            results.extend(validate_frame(_coerce_types(chunk), checks).results)
    return merge_results(results)


def run_suite(engine: Engine, *, since: datetime | None) -> list[CheckResult]:
    variables = load_variables()
    tolerance = load_gold_config().observation_match_tolerance_minutes
    results: list[CheckResult] = []
    for table, columns, stamp, checks in (
        (
            "silver.forecast",
            _FORECAST_SELECT,
            "updated_at",
            forecast_checks(variables, unique=True),
        ),
        (
            "silver.observation",
            _OBSERVATION_SELECT,
            "updated_at",
            observation_checks(variables, unique=True),
        ),
        (
            "gold.forecast_verification",
            _VERIFICATION_SELECT,
            "computed_at",
            verification_checks(variables, tolerance_minutes=tolerance),
        ),
        ("gold.accuracy_daily", _ACCURACY_SELECT, "computed_at", accuracy_checks()),
    ):
        results.extend(validate_table(engine, table, columns, stamp, checks, since))
    results.extend(check_freshness(engine))
    return results


def format_results(results: list[CheckResult]) -> str:
    lines = [f"{'table':<28}{'check':<36}{'severity':<10}{'subject':<22}{'failed':>10}  status"]
    for r in sorted(results, key=lambda r: (r.passed, r.table, r.check, r.subject or "")):
        status = "ok" if r.passed else ("BLOCKING" if r.severity == "blocking" else "warning")
        lines.append(
            f"{r.table:<28}{r.check:<36}{r.severity:<10}{(r.subject or ''):<22}"
            f"{r.rows_failed:>10,}  {status}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the data quality suite.")
    parser.add_argument("--hours", type=float, default=24, help="check rows changed in this window")
    parser.add_argument("--all", action="store_true", help="check every row")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)

    since = None if args.all else datetime.now(UTC) - timedelta(hours=args.hours)
    results = run_suite(engine, since=since)
    persist_results(engine, results, context="quality-suite")
    print(format_results(results))

    if any(r.severity == "blocking" and not r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
