"""range-partition silver.forecast by valid_time (monthly)

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-19

"""

from collections.abc import Sequence
from datetime import date

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The Previous Runs archive begins in January 2024; `make partitions` adds later months.
FIRST_MONTH = date(2024, 1, 1)
LAST_MONTH = date(2027, 12, 1)

_COLUMNS = (
    "model, location_id, init_time, valid_time, variable, value, lead_hours, "
    "ingestion_mode, source_event_id, inserted_at, updated_at"
)


def _months() -> list[tuple[date, date]]:
    months = []
    current = FIRST_MONTH
    while current <= LAST_MONTH:
        following = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
        months.append((current, following))
        current = following
    return months


def _create_partitioned_table() -> None:
    # The primary key must contain the partition key (valid_time) - it already does.
    op.execute(
        """
        CREATE TABLE silver.forecast (
            model text NOT NULL,
            location_id text NOT NULL,
            init_time timestamptz NOT NULL,
            valid_time timestamptz NOT NULL,
            variable text NOT NULL,
            value double precision,
            lead_hours integer NOT NULL,
            ingestion_mode text NOT NULL,
            source_event_id text NOT NULL,
            inserted_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT forecast_pkey
                PRIMARY KEY (model, location_id, init_time, valid_time, variable),
            CONSTRAINT ck_forecast_ingestion_mode CHECK (ingestion_mode in ('live', 'backfill'))
        ) PARTITION BY RANGE (valid_time)
        """
    )
    # Rows outside every monthly range land here rather than failing the insert.
    op.execute("CREATE TABLE silver.forecast_default PARTITION OF silver.forecast DEFAULT")
    for start, end in _months():
        op.execute(
            f"CREATE TABLE silver.forecast_y{start:%Y}m{start:%m} PARTITION OF silver.forecast "
            f"FOR VALUES FROM ('{start}') TO ('{end}')"
        )
    # Defined on the parent, so every partition (present and future) gets them.
    op.create_index(
        "ix_forecast_location_variable_valid_time",
        "forecast",
        ["location_id", "variable", "valid_time"],
        schema="silver",
    )
    op.create_index("ix_forecast_updated_at", "forecast", ["updated_at"], schema="silver")
    op.create_index("ix_forecast_valid_time", "forecast", ["valid_time"], schema="silver")


def upgrade() -> None:
    # Copy-and-swap. Fine for the data volumes this migration meets (a fresh clone,
    # or a dev database); a production table this size would be migrated with a
    # dual-write period instead - noted in ADR 0005.
    op.execute("ALTER TABLE silver.forecast RENAME TO forecast_unpartitioned")
    for index in (
        "ix_forecast_location_variable_valid_time",
        "ix_forecast_updated_at",
        "ix_forecast_valid_time",
    ):
        op.execute(f"DROP INDEX silver.{index}")
    op.execute(
        "ALTER TABLE silver.forecast_unpartitioned "
        "RENAME CONSTRAINT forecast_pkey TO forecast_unpartitioned_pkey"
    )
    op.execute(
        "ALTER TABLE silver.forecast_unpartitioned "
        "RENAME CONSTRAINT ck_forecast_ingestion_mode TO ck_forecast_unpartitioned_mode"
    )

    _create_partitioned_table()
    op.execute(
        f"INSERT INTO silver.forecast ({_COLUMNS}) "
        f"SELECT {_COLUMNS} FROM silver.forecast_unpartitioned"
    )
    op.execute("DROP TABLE silver.forecast_unpartitioned")


def downgrade() -> None:
    op.execute("ALTER TABLE silver.forecast RENAME TO forecast_partitioned")
    op.execute("ALTER TABLE silver.forecast_partitioned RENAME CONSTRAINT forecast_pkey TO fp_pkey")
    op.execute(
        "ALTER TABLE silver.forecast_partitioned "
        "RENAME CONSTRAINT ck_forecast_ingestion_mode TO ck_fp_mode"
    )
    for index in (
        "ix_forecast_location_variable_valid_time",
        "ix_forecast_updated_at",
        "ix_forecast_valid_time",
    ):
        op.execute(f"DROP INDEX silver.{index}")

    op.create_table(
        "forecast",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("location_id", sa.Text(), nullable=False),
        sa.Column("init_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("variable", sa.Text(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("lead_hours", sa.Integer(), nullable=False),
        sa.Column("ingestion_mode", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column(
            "inserted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("model", "location_id", "init_time", "valid_time", "variable"),
        sa.CheckConstraint(
            "ingestion_mode in ('live', 'backfill')", name="ck_forecast_ingestion_mode"
        ),
        schema="silver",
    )
    op.execute(
        f"INSERT INTO silver.forecast ({_COLUMNS}) "
        f"SELECT {_COLUMNS} FROM silver.forecast_partitioned"
    )
    op.execute("DROP TABLE silver.forecast_partitioned")
    op.create_index(
        "ix_forecast_location_variable_valid_time",
        "forecast",
        ["location_id", "variable", "valid_time"],
        schema="silver",
    )
    op.create_index("ix_forecast_updated_at", "forecast", ["updated_at"], schema="silver")
    op.create_index("ix_forecast_valid_time", "forecast", ["valid_time"], schema="silver")
