"""ops.quality_results

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # One row per check outcome (brief section 8). `severity` says what a failure
    # does: 'blocking' rows were rejected (silver: message -> DLQ; gold: build
    # aborted), 'warning' rows were kept and flagged. `context` names the run that
    # produced the result (a consumer batch, a gold day, the quality suite).
    op.create_table(
        "quality_results",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("context", sa.Text(), nullable=False),
        sa.Column("table_name", sa.Text(), nullable=False),
        sa.Column("check_name", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=True),
        sa.Column("rows_checked", sa.BigInteger(), nullable=False),
        sa.Column("rows_failed", sa.BigInteger(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.CheckConstraint("severity in ('blocking', 'warning')", name="ck_quality_severity"),
        schema="ops",
    )
    op.create_index(
        "ix_quality_results_checked_at", "quality_results", ["checked_at"], schema="ops"
    )
    op.create_index(
        "ix_quality_results_check",
        "quality_results",
        ["table_name", "check_name", "checked_at"],
        schema="ops",
    )


def downgrade() -> None:
    op.drop_index("ix_quality_results_check", table_name="quality_results", schema="ops")
    op.drop_index("ix_quality_results_checked_at", table_name="quality_results", schema="ops")
    op.drop_table("quality_results", schema="ops")
