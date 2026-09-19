"""`make partitions`: keep silver.forecast's monthly partitions ahead of the data and
apply the retention policy (config/storage.yaml, ADR 0005).

Creating a partition is idempotent. If rows for that month are already sitting in the
default partition (data older than the migration's range, or written before this job
ran), Postgres refuses to create an overlapping partition, so they are moved first,
in one transaction, into a table that is then attached."""

import argparse
import logging
import re
from datetime import UTC, date, datetime

from sqlalchemy import Connection, Engine, text

from nimbus.common.config import StorageConfig, load_storage_config
from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings

logger = logging.getLogger(__name__)

PARENT = "silver.forecast"
_PARTITION_NAME = re.compile(r"^forecast_y(\d{4})m(\d{2})$")


def add_months(month: date, months: int) -> date:
    index = month.year * 12 + (month.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_start(day: date) -> date:
    return date(day.year, day.month, 1)


def partition_name(month: date) -> str:
    return f"forecast_y{month:%Y}m{month:%m}"


def retention_cutoff(config: StorageConfig, today: date) -> datetime | None:
    """Rows with valid_time before this instant are outside the retention window
    (None = keep everything). Month-aligned so it matches whole partitions."""
    if config.forecast_retention_months is None:
        return None
    first = add_months(month_start(today), -config.forecast_retention_months)
    return datetime(first.year, first.month, first.day, tzinfo=UTC)


def existing_partitions(conn: Connection) -> dict[str, date]:
    """Monthly partitions of silver.forecast by name -> first day of their month."""
    names = conn.execute(
        text(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = CAST(:parent AS regclass)"
        ),
        {"parent": PARENT},
    ).scalars()
    found: dict[str, date] = {}
    for name in names:
        match = _PARTITION_NAME.match(name)
        if match:
            found[name] = date(int(match[1]), int(match[2]), 1)
    return found


def _months_in_default(conn: Connection) -> list[date]:
    lo, hi = conn.execute(
        text("SELECT min(valid_time), max(valid_time) FROM silver.forecast_default")
    ).one()
    if lo is None:
        return []
    first, last = month_start(lo.astimezone(UTC).date()), month_start(hi.astimezone(UTC).date())
    months = []
    while first <= last:
        months.append(first)
        first = add_months(first, 1)
    return months


def _create_partition(conn: Connection, month: date) -> None:
    name, following = partition_name(month), add_months(month, 1)
    bounds = f"FROM ('{month}') TO ('{following}')"
    in_range = f"valid_time >= '{month}' AND valid_time < '{following}'"
    # Names and dates are generated here, never taken from input.
    like = "LIKE silver.forecast INCLUDING DEFAULTS INCLUDING CONSTRAINTS"
    conn.execute(text(f"CREATE TABLE silver.{name} ({like})"))
    conn.execute(
        text(
            f"WITH moved AS (DELETE FROM silver.forecast_default WHERE {in_range} RETURNING *) "
            f"INSERT INTO silver.{name} SELECT * FROM moved"
        )
    )
    conn.execute(text(f"ALTER TABLE silver.forecast ATTACH PARTITION silver.{name} {bounds}"))


def ensure_partitions(engine: Engine, config: StorageConfig, today: date) -> list[str]:
    """Create every missing monthly partition from the current month through
    `partitions_ahead_months`, plus any month that has rows in the default partition."""
    created: list[str] = []
    with engine.begin() as conn:
        present = existing_partitions(conn)
        wanted = {
            add_months(month_start(today), i) for i in range(config.partitions_ahead_months + 1)
        }
        wanted.update(_months_in_default(conn))
        for month in sorted(wanted):
            if partition_name(month) not in present:
                _create_partition(conn, month)
                created.append(partition_name(month))
    return created


def apply_retention(
    engine: Engine, config: StorageConfig, today: date, *, dry_run: bool = False
) -> list[str]:
    """Drop whole monthly partitions that end at or before the retention cutoff."""
    cutoff = retention_cutoff(config, today)
    if cutoff is None:
        return []
    boundary = cutoff.date()
    dropped: list[str] = []
    with engine.begin() as conn:
        for name, month in sorted(existing_partitions(conn).items(), key=lambda kv: kv[1]):
            if add_months(month, 1) <= boundary:
                dropped.append(name)
                if not dry_run:
                    conn.execute(text(f"DROP TABLE silver.{name}"))
    return dropped


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage silver.forecast partitions.")
    parser.add_argument("--dry-run", action="store_true", help="report retention drops only")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)
    config = load_storage_config()
    today = datetime.now(UTC).date()

    created = [] if args.dry_run else ensure_partitions(engine, config, today)
    dropped = apply_retention(engine, config, today, dry_run=args.dry_run)
    verb = "would drop" if args.dry_run else "dropped"
    print(f"created {len(created)} partition(s): {', '.join(created) or '-'}")
    print(f"{verb} {len(dropped)} partition(s): {', '.join(dropped) or '-'}")


if __name__ == "__main__":
    main()
