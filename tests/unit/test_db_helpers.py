from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import not_, or_
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

from nimbus.common import db
from nimbus.common.db import chunked_upsert, nan_to_none, record_ingestion_run
from nimbus.common.tables import observation_table


def _new_dialect() -> Any:
    # SQLAlchemy's dialect constructors are untyped; going through Any keeps
    # mypy strict happy without a blanket ignore.
    factory: Any = PGDialect_psycopg
    return factory()


def _engine() -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine, conn


def test_nan_becomes_none_but_real_values_and_other_types_are_untouched() -> None:
    nan = float("nan")
    records = [{"value": nan, "n": 3, "s": "x", "zero": 0.0, "flag": False, "none": None}]

    assert nan_to_none(records) == [
        {"value": None, "n": 3, "s": "x", "zero": 0.0, "flag": False, "none": None}
    ]


def test_upserted_missing_values_reach_the_driver_as_null_not_nan() -> None:
    """psycopg would send float('nan') as 'NaN'::float8, which `IS NOT NULL`
    accepts and which turns avg() over the column into NaN."""
    engine, conn = _engine()
    row = {
        "station": "K",
        "observed_at": datetime(2026, 1, 1, tzinfo=UTC),
        "variable": "v",
        "value": float("nan"),
        "raw_text": "t",
        "is_corrected": False,
        "ingestion_mode": "live",
        "source_event_id": "e",
    }

    chunked_upsert(
        engine, observation_table, ["station", "observed_at", "variable"], ["value"], [row]
    )

    params = conn.execute.call_args[0][0].compile(dialect=_new_dialect()).params
    assert params["value_m0"] is None


def test_the_update_guard_is_emitted_as_a_where_on_do_update() -> None:
    engine, conn = _engine()
    row = {
        "station": "K",
        "observed_at": datetime(2026, 1, 1, tzinfo=UTC),
        "variable": "v",
        "value": 1.0,
        "raw_text": "t",
        "is_corrected": False,
        "ingestion_mode": "live",
        "source_event_id": "e",
    }

    def guard(excluded: Any) -> Any:
        return or_(excluded.is_corrected, not_(observation_table.c.is_corrected))

    chunked_upsert(
        engine,
        observation_table,
        ["station", "observed_at", "variable"],
        ["value"],
        [row],
        update_where=guard,
    )

    sql = str(conn.execute.call_args[0][0].compile(dialect=_new_dialect()))
    assert "WHERE excluded.is_corrected OR NOT silver.observation.is_corrected" in sql


def test_without_a_guard_the_update_is_unconditional() -> None:
    engine, conn = _engine()
    row = {
        "station": "K",
        "observed_at": datetime(2026, 1, 1, tzinfo=UTC),
        "variable": "v",
        "value": 1.0,
        "raw_text": "t",
        "is_corrected": False,
        "ingestion_mode": "live",
        "source_event_id": "e",
    }

    chunked_upsert(
        engine, observation_table, ["station", "observed_at", "variable"], ["value"], [row]
    )

    sql = str(conn.execute.call_args[0][0].compile(dialect=_new_dialect()))
    assert "DO UPDATE SET value = excluded.value" in sql
    assert " WHERE " not in sql


def _recorded_params(**kwargs: Any) -> dict[str, Any]:
    engine, conn = _engine()
    record_ingestion_run(engine, "src", "backfill", datetime(2026, 1, 1, tzinfo=UTC), 5, **kwargs)
    return dict(conn.execute.call_args[0][1])


def test_a_run_with_an_error_message_is_recorded_as_failed_even_with_zero_failures() -> None:
    params = _recorded_params(
        failed=0, error_message="rate limited; resume with --start-date 2025-03-01"
    )

    assert params["status"] == "failed"
    assert params["error_message"].startswith("rate limited")


def test_a_clean_run_is_still_recorded_as_success() -> None:
    params = _recorded_params(failed=0)

    assert (params["status"], params["error_message"]) == ("success", None)
    assert db.UPSERT_CHUNK_SIZE == 5000  # documented in ADR 0002
