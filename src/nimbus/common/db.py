"""SQLAlchemy Core engine/session helpers (psycopg 3 driver)."""

import math
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import ColumnElement, Engine, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from nimbus.common.settings import Settings

# Postgres caps a single statement at 65535 bound parameters (see ADR 0002 -
# found by running the real pipeline at realistic batch size, not a unit
# test). Chunking well below that per statement keeps every bulk upsert safe
# regardless of how wide the target table is.
UPSERT_CHUNK_SIZE = 5000


def make_engine(settings: Settings, *, readonly: bool = False) -> Engine:
    dsn = settings.postgres_readonly_dsn if readonly else settings.postgres_dsn
    return create_engine(dsn, pool_pre_ping=True)


def record_ingestion_run(
    engine: Engine,
    source: str,
    ingestion_mode: str,
    started_at: datetime,
    produced: int,
    failed: int,
    error_message: str | None = None,
) -> None:
    """Every producer's per-run metrics (brief section 6: `ops.ingestion_runs`).
    A run that stopped early passes `error_message`, which marks it failed even
    when no individual request failed (e.g. it was rate limited part-way)."""
    status = "success" if failed == 0 and error_message is None else "failed"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.ingestion_runs "
                "(source, ingestion_mode, status, started_at, finished_at, "
                "messages_produced, messages_failed, error_message) "
                "VALUES (:source, :mode, :status, :started_at, :finished_at, :produced, :failed, "
                ":error_message)"
            ),
            {
                "source": source,
                "mode": ingestion_mode,
                "status": status,
                "started_at": started_at,
                "finished_at": datetime.now(UTC),
                "produced": produced,
                "failed": failed,
                "error_message": error_message,
            },
        )


def nan_to_none(records: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
    """Missing floats must reach Postgres as NULL, not NaN. pandas' to_dict()
    yields float('nan'), and psycopg sends that as 'NaN'::float8 - a value that
    `IS NOT NULL` accepts and that turns any avg()/sum() over the column into
    NaN. The value columns are nullable precisely so "missing" is NULL."""
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in record.items()}
        for record in records
    ]


def chunked_upsert(
    engine: Engine,
    table: Table,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
    # pandas' DataFrame.to_dict("records") types keys as Hashable, not str -
    # accept that directly rather than making every caller re-cast records
    # built straight from a DataFrame.
    records: list[dict[Any, Any]],
    *,
    chunk_size: int = UPSERT_CHUNK_SIZE,
    update_where: Callable[[Any], ColumnElement[bool]] | None = None,
) -> None:
    """INSERT ... ON CONFLICT DO UPDATE, chunked to stay under Postgres's
    bound-parameter limit. Shared by every silver consumer's load step.

    `update_where` receives the `excluded` row and returns a condition that must
    hold for an existing row to be overwritten - used to stop a stale report from
    clobbering a newer one already stored (see observation_silver)."""
    if not records:
        return
    records = nan_to_none(records)
    with engine.begin() as conn:
        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = pg_insert(table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={col: getattr(stmt.excluded, col) for col in update_columns},
                where=update_where(stmt.excluded) if update_where is not None else None,
            )
            conn.execute(stmt)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dedupe_on_key(rows: pd.DataFrame, key_columns: Sequence[str]) -> pd.DataFrame:
    """Keep the last row per natural key. Postgres rejects an INSERT ... ON
    CONFLICT DO UPDATE that would touch the same row twice in one statement
    ("cannot affect row a second time"), which a batch containing both an
    original report and its correction - or two overlapping backfill windows -
    would otherwise trigger. Last-wins matches arrival order."""
    return rows.drop_duplicates(subset=list(key_columns), keep="last")
