"""ops.reconciliation_results table

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-18

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # One row per reconciliation run per topic (brief section 8): what was
    # produced, what bronze captured, what silver *should* contain if rebuilt
    # from bronze, and what it actually contains.
    op.create_table(
        "reconciliation_results",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("produced_messages", sa.BigInteger(), nullable=False),
        sa.Column("bronze_messages", sa.BigInteger(), nullable=False),
        sa.Column("bronze_distinct_events", sa.BigInteger(), nullable=False),
        sa.Column("bronze_unparseable", sa.BigInteger(), nullable=False),
        sa.Column("expected_silver_rows", sa.BigInteger(), nullable=False),
        sa.Column("silver_rows", sa.BigInteger(), nullable=False),
        sa.Column("missing_from_silver", sa.BigInteger(), nullable=False),
        sa.Column("extra_in_silver", sa.BigInteger(), nullable=False),
        sa.Column("matched", sa.Boolean(), nullable=False),
        schema="ops",
    )


def downgrade() -> None:
    op.drop_table("reconciliation_results", schema="ops")
