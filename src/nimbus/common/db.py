"""SQLAlchemy Core engine/session helpers (psycopg 3 driver)."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from nimbus.common.settings import Settings


def make_engine(settings: Settings, *, readonly: bool = False) -> Engine:
    dsn = settings.postgres_readonly_dsn if readonly else settings.postgres_dsn
    return create_engine(dsn, pool_pre_ping=True)


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
