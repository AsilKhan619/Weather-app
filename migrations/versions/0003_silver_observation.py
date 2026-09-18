"""silver.observation table

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-18

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Natural key is (station, observed_at, variable) - brief section 7.
    # A correction (COR) for the same slot upserts over the original,
    # which is exactly "keeps the latest version of corrected reports":
    # raw_text/is_corrected are not part of the key, so there is only ever
    # one row per (station, observed_at, variable).
    op.create_table(
        "observation",
        sa.Column("station", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("variable", sa.Text(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("is_corrected", sa.Boolean(), nullable=False),
        sa.Column("ingestion_mode", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column(
            "inserted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("station", "observed_at", "variable"),
        sa.CheckConstraint(
            "ingestion_mode in ('live', 'backfill')", name="ck_observation_ingestion_mode"
        ),
        schema="silver",
    )
    op.create_index(
        "ix_observation_station_variable_observed_at",
        "observation",
        ["station", "variable", "observed_at"],
        schema="silver",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_observation_station_variable_observed_at", table_name="observation", schema="silver"
    )
    op.drop_table("observation", schema="silver")
