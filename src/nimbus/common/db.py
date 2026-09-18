"""SQLAlchemy Core engine/session helpers (psycopg 3 driver)."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Engine, Table, create_engine, text
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
) -> None:
    """Every producer's per-run metrics (brief section 6: `ops.ingestion_runs`)."""
    status = "success" if failed == 0 else "failed"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.ingestion_runs "
                "(source, ingestion_mode, status, started_at, finished_at, "
                "messages_produced, messages_failed) "
                "VALUES (:source, :mode, :status, :started_at, :finished_at, :produced, :failed)"
            ),
            {
                "source": source,
                "mode": ingestion_mode,
                "status": status,
                "started_at": started_at,
                "finished_at": datetime.now(UTC),
                "produced": produced,
                "failed": failed,
            },
        )


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
) -> None:
    """INSERT ... ON CONFLICT DO UPDATE, chunked to stay under Postgres's
    bound-parameter limit. Shared by every silver consumer's load step."""
    if not records:
        return
    with engine.begin() as conn:
        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = pg_insert(table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={col: getattr(stmt.excluded, col) for col in update_columns},
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
