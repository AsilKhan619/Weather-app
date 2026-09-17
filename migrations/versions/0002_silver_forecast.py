"""silver.forecast table

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-17

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # No dim_location/dim_model/dim_variable tables yet - Phase 1 only needs
    # plain natural-key columns; dimension tables get added when a later
    # phase actually needs to join richer attributes (dashboard, Phase 4).
    # Monthly partitioning by valid_time is deferred to Phase 3, once real
    # volume (see ADR 0001) makes it worth the added complexity.
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
            "inserted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("model", "location_id", "init_time", "valid_time", "variable"),
        sa.CheckConstraint(
            "ingestion_mode in ('live', 'backfill')", name="ck_forecast_ingestion_mode"
        ),
        schema="silver",
    )
    op.create_index(
        "ix_forecast_location_variable_valid_time",
        "forecast",
        ["location_id", "variable", "valid_time"],
        schema="silver",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_forecast_location_variable_valid_time", table_name="forecast", schema="silver"
    )
    op.drop_table("forecast", schema="silver")
