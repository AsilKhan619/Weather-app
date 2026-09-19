"""Incremental, idempotent gold build (brief sections 7, 8; ADR 0005).

The unit of work is one UTC *valid date*: read that day's forecasts and the
observations around it, verify, aggregate, and reconcile the result into
`gold.forecast_verification` and `gold.accuracy_daily` in one transaction.

Incremental: a watermark on `updated_at` (bumped by the silver upserts only when a
row really changed) selects the days that saw new or revised data since the last
build - minus a lookback, because a transaction that started before the snapshot
can commit after it. A changed forecast dirties its valid date; a changed
observation dirties its date and both neighbours, since a match can cross midnight.

Idempotent: a day is *synced*, not appended - new rows are upserted only where a
value differs, and rows the day no longer produces (an observation corrected away,
a forecast withdrawn) are deleted. Rebuilding an unchanged day writes nothing, so
a re-run leaves every table byte-for-byte as it was."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import pandas as pd
from sqlalchemy import Connection, Engine, Table, and_, delete, func, select, text, tuple_

from nimbus.common.config import GoldConfig, load_gold_config, load_variables
from nimbus.common.db import UPSERT_CHUNK_SIZE, upsert_chunks
from nimbus.common.tables import (
    accuracy_daily_table,
    dim_location_table,
    forecast_table,
    forecast_verification_table,
    gold_build_log_table,
    job_state_table,
    observation_table,
)
from nimbus.gold.verification import (
    aggregate_accuracy,
    eligible_forecasts,
    verify_forecasts,
)
from nimbus.quality.runner import CheckResult, QualityError, persist_results, validate_frame
from nimbus.quality.schemas import accuracy_checks, verification_checks

logger = logging.getLogger(__name__)

JOB_NAME = "gold_build"

_VERIFICATION_KEY = ["model", "location_id", "init_time", "valid_time", "variable"]
_VERIFICATION_VALUES = [
    "lead_hours",
    "lead_day",
    "forecast_value",
    "observed_value",
    "observed_at",
    "obs_offset_seconds",
    "error",
    "ingestion_mode",
]
_ACCURACY_KEY = ["valid_date", "location_id", "model", "variable", "lead_day"]
_ACCURACY_VALUES = ["n", "bias", "mae", "rmse"]

# Deleting stale keys binds every key column of every row; keep well under 65,535.
_DELETE_CHUNK = 5000

RunKind = Literal["incremental", "full"]


@dataclass(frozen=True)
class DayResult:
    valid_date: date
    eligible: int
    matched: int
    quality: list[CheckResult]


@dataclass(frozen=True)
class BuildResult:
    run_kind: RunKind
    days: list[DayResult]
    watermark: datetime

    @property
    def matched(self) -> int:
        return sum(d.matched for d in self.days)


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def _utc(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def read_watermark(conn: Connection) -> datetime | None:
    return conn.execute(
        select(job_state_table.c.watermark).where(job_state_table.c.job == JOB_NAME)
    ).scalar()


def days_to_build(conn: Connection, *, since: datetime | None, tolerance: timedelta) -> list[date]:
    """UTC valid dates whose gold rows may be out of date.

    `since=None` means every day that has both forecasts and observations to match.
    Days after the newest observation cannot have any match yet; they are skipped and
    picked up when the observation that makes them verifiable arrives."""
    latest_obs = conn.execute(select(func.max(observation_table.c.observed_at))).scalar()
    if latest_obs is None:
        return []
    limit = (latest_obs + tolerance).astimezone(UTC).date()

    if since is None:
        lo, hi = conn.execute(
            select(func.min(forecast_table.c.valid_time), func.max(forecast_table.c.valid_time))
        ).one()
        if lo is None:
            return []
        first = lo.astimezone(UTC).date()
        last = min(hi.astimezone(UTC).date(), limit)
        return [first + timedelta(days=i) for i in range((last - first).days + 1)]

    forecast_days = conn.execute(
        text(
            "SELECT DISTINCT (valid_time AT TIME ZONE 'UTC')::date "
            "FROM silver.forecast WHERE updated_at >= :since"
        ),
        {"since": since},
    ).scalars()
    observation_days = conn.execute(
        text(
            "SELECT DISTINCT (observed_at AT TIME ZONE 'UTC')::date "
            "FROM silver.observation WHERE updated_at >= :since"
        ),
        {"since": since},
    ).scalars()
    dirty = set(forecast_days)
    for day in observation_days:
        dirty.update((day - timedelta(days=1), day, day + timedelta(days=1)))
    return sorted(d for d in dirty if d <= limit)


def _key(values: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(v.to_pydatetime() if isinstance(v, pd.Timestamp) else v for v in values)


def sync_rows(
    conn: Connection,
    table: Table,
    key_columns: Sequence[str],
    value_columns: Sequence[str],
    frame: pd.DataFrame,
    scope: Any,
) -> None:
    """Make the rows of `table` matching `scope` equal to `frame`: upsert what
    differs, delete what is no longer produced. Nothing is written for rows that
    are already correct."""
    existing = {
        _key(row)
        for row in conn.execute(select(*(table.c[c] for c in key_columns)).where(scope)).all()
    }
    records = frame.to_dict("records")
    upsert_chunks(
        conn,
        table,
        key_columns,
        value_columns,
        records,
        chunk_size=UPSERT_CHUNK_SIZE,
        only_if_changed=True,
        touch_columns=("computed_at",),
    )
    produced = {_key([record[c] for c in key_columns]) for record in records}
    stale = sorted(existing - produced, key=repr)
    for start in range(0, len(stale), _DELETE_CHUNK):
        chunk = stale[start : start + _DELETE_CHUNK]
        conn.execute(
            delete(table).where(and_(scope, tuple_(*(table.c[c] for c in key_columns)).in_(chunk)))
        )


_FORECAST_COLUMNS = [
    "model",
    "location_id",
    "init_time",
    "valid_time",
    "variable",
    "value",
    "lead_hours",
    "ingestion_mode",
]


def _read_forecasts(conn: Connection, start: datetime, end: datetime) -> pd.DataFrame:
    t = forecast_table.c
    stmt = select(*(t[c] for c in _FORECAST_COLUMNS)).where(
        and_(t.valid_time >= start, t.valid_time < end)
    )
    frame = pd.DataFrame(conn.execute(stmt).mappings().all(), columns=_FORECAST_COLUMNS)
    if frame.empty:
        return frame
    return _utc(frame.astype({"value": "float64"}), ["init_time", "valid_time"])


def _read_observations(
    conn: Connection, start: datetime, end: datetime, tolerance: timedelta
) -> pd.DataFrame:
    t = observation_table.c
    stmt = select(t.station, t.observed_at, t.variable, t.value).where(
        and_(t.observed_at >= start - tolerance, t.observed_at < end + tolerance)
    )
    frame = pd.DataFrame(conn.execute(stmt).mappings().all())
    if frame.empty:
        return pd.DataFrame(columns=["station", "observed_at", "variable", "value"])
    return _utc(frame.astype({"value": "float64"}), ["observed_at"])


def build_day(conn: Connection, day: date, config: GoldConfig, run_kind: RunKind) -> DayResult:
    """Recompute one valid date and reconcile it into gold, inside the caller's
    transaction so a day is never half-written."""
    start, end = _day_bounds(day)
    tolerance = timedelta(minutes=config.observation_match_tolerance_minutes)

    forecasts = _read_forecasts(conn, start, end)
    observations = _read_observations(conn, start, end, tolerance)
    stations = pd.DataFrame(
        conn.execute(select(dim_location_table.c.location_id, dim_location_table.c.station))
        .mappings()
        .all(),
        columns=["location_id", "station"],
    )

    verification = verify_forecasts(
        forecasts,
        observations,
        stations,
        tolerance_minutes=config.observation_match_tolerance_minutes,
        min_lead_hours=config.min_lead_hours,
    )
    accuracy = aggregate_accuracy(verification)

    # Quality gate before the load (brief section 8): a blocking failure aborts the
    # day - gold keeps its previous, valid rows - and the run fails loudly.
    quality = [
        *validate_frame(
            verification,
            verification_checks(
                load_variables(), tolerance_minutes=config.observation_match_tolerance_minutes
            ),
        ).results,
        *validate_frame(accuracy, accuracy_checks()).results,
    ]
    blocking = [r for r in quality if r.severity == "blocking" and not r.passed]
    if blocking:
        names = ", ".join(sorted({f"{r.table}:{r.check}" for r in blocking}))
        raise QualityError(f"gold build for {day} failed blocking checks: {names}", quality)

    v = forecast_verification_table.c
    sync_rows(
        conn,
        forecast_verification_table,
        _VERIFICATION_KEY,
        _VERIFICATION_VALUES,
        verification,
        and_(v.valid_time >= start, v.valid_time < end),
    )
    sync_rows(
        conn,
        accuracy_daily_table,
        _ACCURACY_KEY,
        _ACCURACY_VALUES,
        accuracy,
        accuracy_daily_table.c.valid_date == day,
    )

    eligible = len(eligible_forecasts(forecasts, min_lead_hours=config.min_lead_hours))
    conn.execute(
        gold_build_log_table.insert().values(
            run_kind=run_kind,
            valid_date=day,
            eligible_forecasts=eligible,
            matched=len(verification),
        )
    )
    return DayResult(valid_date=day, eligible=eligible, matched=len(verification), quality=quality)


def build_gold(
    engine: Engine, *, full: bool = False, config: GoldConfig | None = None
) -> BuildResult:
    """Build every stale day, then advance the watermark. The watermark only moves
    after all days succeed, so a failed run is simply retried."""
    config = config or load_gold_config()
    tolerance = timedelta(minutes=config.observation_match_tolerance_minutes)

    with engine.connect() as conn:
        # Snapshot before reading anything: rows landing while we build are picked up
        # by the next run rather than lost between "read" and "advance".
        snapshot: datetime = conn.execute(text("SELECT now()")).scalar_one()
        watermark = read_watermark(conn)
        since = (
            None
            if full or watermark is None
            else watermark - timedelta(hours=config.lookback_hours)
        )
        days = days_to_build(conn, since=since, tolerance=tolerance)

    run_kind: RunKind = "full" if since is None else "incremental"
    results: list[DayResult] = []
    for day in days:
        try:
            with engine.begin() as conn:
                result = build_day(conn, day, config, run_kind)
        except QualityError as exc:
            persist_results(engine, exc.results, context=f"gold-build {day}")
            raise
        persist_results(engine, result.quality, context=f"gold-build {day}")
        results.append(result)
        logger.info(
            "gold day built",
            extra={
                "valid_date": day.isoformat(),
                "eligible": result.eligible,
                "matched": result.matched,
                "run_kind": run_kind,
            },
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.job_state (job, watermark) VALUES (:job, :wm) "
                "ON CONFLICT (job) DO UPDATE SET watermark = excluded.watermark, "
                "updated_at = now()"
            ),
            {"job": JOB_NAME, "wm": snapshot},
        )
    return BuildResult(run_kind=run_kind, days=results, watermark=snapshot)
