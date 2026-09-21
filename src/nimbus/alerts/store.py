"""Postgres side of the detector. The detector keeps *no* in-memory state: everything it
compares a new event against - the model's previous run, the other models' latest runs,
the forecasts an observation should be judged by - is read from silver (live rows only:
backfilled `init_time`s are derived, not real runs). A restart therefore loses nothing
(ADR 0006).

`ForecastLookup` is the interface the detector needs, so unit tests can supply an
in-memory fake."""

import json
from datetime import datetime, timedelta
from typing import Protocol

import pandas as pd
from sqlalchemy import Engine, text

from nimbus.common.schemas import AlertPayload

_COLUMNS = ["variable", "valid_time", "value"]


class ForecastLookup(Protocol):
    def previous_run(
        self, model: str, location_id: str, init: datetime, hours: int
    ) -> tuple[datetime, pd.DataFrame] | None: ...

    def latest_run(
        self, model: str, location_id: str, init: datetime, hours: int, cadence_hours: int
    ) -> tuple[datetime, pd.DataFrame] | None: ...

    def forecasts_at(
        self, location_id: str, variable: str, at: datetime, max_lead_hours: int
    ) -> dict[str, float]: ...


def _frame(rows: list[tuple[str, datetime, float | None]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=_COLUMNS)
    frame["valid_time"] = pd.to_datetime(frame["valid_time"], utc=True)
    frame["value"] = frame["value"].astype("float64")
    return frame


class PostgresForecasts:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def _run_frame(
        self, model: str, location_id: str, run_init: datetime, init: datetime, hours: int
    ) -> pd.DataFrame:
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT variable, valid_time, value FROM silver.forecast "
                    "WHERE model = :model AND location_id = :loc AND init_time = :run_init "
                    "AND valid_time > :init AND valid_time <= :end"
                ),
                {
                    "model": model,
                    "loc": location_id,
                    "run_init": run_init,
                    "init": init,
                    "end": init + timedelta(hours=hours),
                },
            ).all()
        return _frame([(str(v), t, x) for v, t, x in rows])

    def previous_run(
        self, model: str, location_id: str, init: datetime, hours: int
    ) -> tuple[datetime, pd.DataFrame] | None:
        """The model's most recent live run before `init`, over the next `hours` from `init`."""
        with self._engine.connect() as conn:
            previous = conn.execute(
                text(
                    "SELECT max(init_time) FROM silver.forecast "
                    "WHERE model = :model AND location_id = :loc "
                    "AND ingestion_mode = 'live' AND init_time < :init"
                ),
                {"model": model, "loc": location_id, "init": init},
            ).scalar()
        if previous is None:
            return None
        return previous, self._run_frame(model, location_id, previous, init, hours)

    def latest_run(
        self, model: str, location_id: str, init: datetime, hours: int, cadence_hours: int
    ) -> tuple[datetime, pd.DataFrame] | None:
        """The model's newest live run no older than one cadence before `init` - a model
        that is a whole cycle behind is not comparable and is left out."""
        with self._engine.connect() as conn:
            newest = conn.execute(
                text(
                    "SELECT max(init_time) FROM silver.forecast "
                    "WHERE model = :model AND location_id = :loc "
                    "AND ingestion_mode = 'live' AND init_time >= :oldest"
                ),
                {
                    "model": model,
                    "loc": location_id,
                    "oldest": init - timedelta(hours=cadence_hours),
                },
            ).scalar()
        if newest is None:
            return None
        return newest, self._run_frame(model, location_id, newest, init, hours)

    def forecasts_at(
        self, location_id: str, variable: str, at: datetime, max_lead_hours: int
    ) -> dict[str, float]:
        """Each model's latest live short-range forecast for the hour `at`."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ON (model) model, value FROM silver.forecast "
                    "WHERE location_id = :loc AND variable = :variable "
                    "AND ingestion_mode = 'live' AND valid_time = :at AND init_time <= :at "
                    "AND lead_hours BETWEEN 1 AND :max_lead AND value IS NOT NULL "
                    "ORDER BY model, init_time DESC"
                ),
                {"loc": location_id, "variable": variable, "at": at, "max_lead": max_lead_hours},
            ).all()
        return {str(model): float(value) for model, value in rows}


def insert_alerts(engine: Engine, alerts: list[AlertPayload]) -> None:
    """Record alerts; one that already exists (same deterministic id) is left alone."""
    if not alerts:
        return
    with engine.begin() as conn:
        for alert in alerts:
            conn.execute(
                text(
                    "INSERT INTO gold.alert (alert_id, rule, severity, location_id, variable, "
                    "model, station, event_time, metric, threshold, details, "
                    "triggered_by_event_id, detected_at) "
                    "VALUES (:alert_id, :rule, :severity, :location_id, :variable, :model, "
                    ":station, :event_time, :metric, :threshold, CAST(:details AS jsonb), "
                    ":triggered_by_event_id, :detected_at) ON CONFLICT (alert_id) DO NOTHING"
                ),
                {
                    **alert.model_dump(exclude={"details"}),
                    "details": json.dumps(alert.details, default=str),
                },
            )


def unpublished(engine: Engine, alert_ids: list[str]) -> set[str]:
    """Which of these alerts have not yet been confirmed on the topic."""
    if not alert_ids:
        return set()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT alert_id FROM gold.alert "
                "WHERE alert_id = ANY(:ids) AND published_at IS NULL"
            ),
            {"ids": alert_ids},
        ).scalars()
        return {str(r) for r in rows}


def mark_published(engine: Engine, alert_ids: list[str]) -> None:
    if not alert_ids:
        return
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE gold.alert SET published_at = now() WHERE alert_id = ANY(:ids)"),
            {"ids": alert_ids},
        )
