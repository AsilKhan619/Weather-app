"""silver.forecast is range-partitioned by valid_time (ADR 0005): upserts still
work, rows land in the right month, missing months are created (adopting rows that
had fallen into the default partition), and retention drops whole months."""

from collections.abc import Iterator
from datetime import UTC, date, datetime

import pandas as pd
import pytest
from sqlalchemy import Engine, text

from nimbus.common.config import StorageConfig
from nimbus.common.db import make_engine
from nimbus.common.settings import Settings
from nimbus.jobs.manage_partitions import apply_retention, ensure_partitions
from nimbus.streaming.forecast_silver import upsert_forecast_rows

pytestmark = pytest.mark.integration

_INIT = datetime(2023, 1, 1, tzinfo=UTC)


@pytest.fixture
def engine(pg_settings: Settings) -> Iterator[Engine]:
    eng = make_engine(pg_settings)
    _truncate(eng)
    yield eng
    _truncate(eng)
    eng.dispose()


def _truncate(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.forecast"))


def _rows(*valid_times: datetime, value: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "model": "gfs_seamless",
            "location_id": "sfo",
            "init_time": _INIT,
            "valid_time": list(valid_times),
            "variable": "temperature_2m",
            "value": value,
            "lead_hours": 24,
            "ingestion_mode": "backfill",
            "source_event_id": "e",
        }
    )


def _by_partition(engine: Engine) -> dict[str, int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT tableoid::regclass::text, count(*) FROM silver.forecast GROUP BY 1")
        )
        return {name.removeprefix("silver."): int(count) for name, count in rows}


def _partitions(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        return set(
            conn.execute(
                text(
                    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                    "WHERE i.inhparent = 'silver.forecast'::regclass"
                )
            ).scalars()
        )


def test_the_table_is_partitioned_monthly_with_a_default(engine: Engine) -> None:
    with engine.connect() as conn:
        kind = conn.execute(
            text("SELECT relkind FROM pg_class WHERE oid = 'silver.forecast'::regclass")
        ).scalar_one()
    partitions = _partitions(engine)

    assert kind == "p"
    assert {"forecast_default", "forecast_y2024m01", "forecast_y2027m12"} <= partitions
    assert len(partitions) >= 48 + 1  # 48 months + default (tests share this database)


def test_upserts_route_to_the_right_month_and_still_conflict_on_the_key(engine: Engine) -> None:
    jan, feb = datetime(2026, 1, 15, 12, tzinfo=UTC), datetime(2026, 2, 3, tzinfo=UTC)
    upsert_forecast_rows(engine, _rows(jan, feb, value=1.0))
    upsert_forecast_rows(engine, _rows(jan, feb, value=2.0))  # same keys, new value

    counts = _by_partition(engine)
    assert counts["forecast_y2026m01"] == 1 and counts["forecast_y2026m02"] == 1
    with engine.connect() as conn:
        values = set(conn.execute(text("SELECT value FROM silver.forecast")).scalars())
    assert values == {2.0}  # updated in place across partitions, not duplicated
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.forecast"))


def test_ensure_partitions_creates_upcoming_months_and_adopts_default_rows(
    engine: Engine,
) -> None:
    old = datetime(2023, 6, 10, tzinfo=UTC)  # before the migration's first month
    upsert_forecast_rows(engine, _rows(old))
    assert _by_partition(engine) == {"forecast_default": 1}

    config = StorageConfig(partitions_ahead_months=2)
    created = ensure_partitions(engine, config, date(2028, 3, 15))

    assert created == [
        "forecast_y2023m06",
        "forecast_y2028m03",
        "forecast_y2028m04",
        "forecast_y2028m05",
    ]
    assert _by_partition(engine) == {"forecast_y2023m06": 1}  # moved out of default
    assert ensure_partitions(engine, config, date(2028, 3, 15)) == []  # idempotent

    upsert_forecast_rows(engine, _rows(datetime(2028, 4, 2, tzinfo=UTC)))
    assert _by_partition(engine)["forecast_y2028m04"] == 1
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.forecast"))


def test_retention_drops_whole_months_older_than_the_window(engine: Engine) -> None:
    old = datetime(2025, 8, 20, tzinfo=UTC)
    boundary_month = datetime(2025, 9, 5, tzinfo=UTC)  # first retained month
    recent = datetime(2026, 9, 1, tzinfo=UTC)
    upsert_forecast_rows(engine, _rows(old, boundary_month, recent))
    config = StorageConfig(partitions_ahead_months=6, forecast_retention_months=12)
    today = date(2026, 9, 19)  # keep 2025-09 .. 2026-09

    would_drop = apply_retention(engine, config, today, dry_run=True)
    assert "forecast_y2025m08" in would_drop and "forecast_y2025m09" not in would_drop
    assert _by_partition(engine)["forecast_y2025m08"] == 1  # dry run touched nothing

    dropped = apply_retention(engine, config, today)

    assert dropped == would_drop
    assert "forecast_y2024m01" in dropped and dropped[-1] == "forecast_y2025m08"
    assert dropped == sorted(dropped)
    assert _by_partition(engine) == {"forecast_y2025m09": 1, "forecast_y2026m09": 1}
    assert apply_retention(engine, config, today) == []  # nothing left to drop
    assert apply_retention(engine, StorageConfig(partitions_ahead_months=6), today) == []
