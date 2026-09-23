"""gold.briefing and ops.llm_calls

Revision ID: 0010
Revises: 0009
Create Date: 2026-09-23

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # One row per *distinct input*: briefing_id is a hash of (fact sheet, prompt version,
    # model), so an identical request finds its answer here instead of paying for a new
    # call (brief section 10, ADR 0008). The triggers that asked for it are not the key -
    # a daily run and an alert that see the same facts share one briefing.
    op.create_table(
        "briefing",
        sa.Column("briefing_id", sa.Text(), primary_key=True),
        sa.Column("location_id", sa.Text(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("trigger_ref", sa.Text(), nullable=True),
        sa.Column("fact_sheet_hash", sa.Text(), nullable=False),
        sa.Column("fact_sheet", postgresql.JSONB(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Text(), nullable=False),
        sa.Column("most_reliable_model", sa.Text(), nullable=False),
        sa.Column("reliable_reason", sa.Text(), nullable=False),
        sa.Column("notable_risks", postgresql.JSONB(), nullable=False),
        # Failing the grounding check stores the briefing flagged and never publishes it.
        sa.Column("grounding_passed", sa.Boolean(), nullable=False),
        sa.Column("grounding_failures", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("trigger in ('daily', 'alert', 'manual')", name="ck_briefing_trigger"),
        sa.CheckConstraint(
            "confidence in ('high', 'medium', 'low')", name="ck_briefing_confidence"
        ),
        schema="gold",
    )
    op.create_index(
        "ix_briefing_location_as_of", "briefing", ["location_id", "as_of"], schema="gold"
    )

    # Every LLM request, and every request that did not need one (a cache hit, LLM
    # disabled), so cost and failure rates are measurable (brief section 10).
    op.create_table(
        "llm_calls",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "called_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("fact_sheet_hash", sa.Text(), nullable=True),
        sa.Column("attempt", sa.SmallInteger(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("cache_read_tokens", sa.Integer(), nullable=False),
        sa.Column("cache_write_tokens", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("cost_usd", sa.Float(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "outcome in ('success', 'invalid_output', 'grounding_failed', 'error', "
            "'cache_hit', 'disabled')",
            name="ck_llm_calls_outcome",
        ),
        schema="ops",
    )
    op.create_index("ix_llm_calls_called_at", "llm_calls", ["called_at"], schema="ops")


def downgrade() -> None:
    op.drop_index("ix_llm_calls_called_at", table_name="llm_calls", schema="ops")
    op.drop_table("llm_calls", schema="ops")
    op.drop_index("ix_briefing_location_as_of", table_name="briefing", schema="gold")
    op.drop_table("briefing", schema="gold")
