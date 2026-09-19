"""silver dimensions + change tracking (updated_at)

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Verification must join a forecast's location_id to the observing station,
    # and that mapping only lived in config/locations.yaml. The brief's silver
    # model includes these dimensions; they became necessary with the gold layer
    # (ADR 0005). Populated idempotently from config by nimbus.jobs.load_dimensions.
    op.create_table(
        "dim_location",
        sa.Column("location_id", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("climate", sa.Text(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("elevation_m", sa.Float(), nullable=False),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("station", sa.Text(), nullable=False, unique=True),
        schema="silver",
    )
    op.create_table(
        "dim_model",
        sa.Column("model_id", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        schema="silver",
    )
    op.create_table(
        "dim_variable",
        sa.Column("variable", sa.Text(), primary_key=True),
        sa.Column("unit", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        schema="silver",
    )

    # Incremental gold builds recompute only what changed. `inserted_at` is set
    # once, so an upsert that *changes* a row (a correction, a revised forecast,
    # a late report) was invisible to change detection. `updated_at` is set on
    # insert and bumped only when an upsert actually changes the row.
    for table in ("forecast", "observation"):
        op.add_column(
            table,
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            schema="silver",
        )
        op.create_index(f"ix_{table}_updated_at", table, ["updated_at"], schema="silver")

    # Gold reads one valid_time day at a time; the existing indexes lead with
    # location/station, so without this every day's read would scan the table.
    op.create_index("ix_forecast_valid_time", "forecast", ["valid_time"], schema="silver")


def downgrade() -> None:
    op.drop_index("ix_forecast_valid_time", table_name="forecast", schema="silver")
    for table in ("observation", "forecast"):
        op.drop_index(f"ix_{table}_updated_at", table_name=table, schema="silver")
        op.drop_column(table, "updated_at", schema="silver")
    op.drop_table("dim_variable", schema="silver")
    op.drop_table("dim_model", schema="silver")
    op.drop_table("dim_location", schema="silver")
