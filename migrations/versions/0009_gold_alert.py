"""gold.alert: the anomaly detector's alerts, kept queryable

Revision ID: 0009
Revises: 0008
Create Date: 2026-09-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # weather.alert.v1 is the transport; this table is what the dashboard, the
    # briefings and the agent read. The detector inserts the row first and marks it
    # published only after the Kafka delivery is confirmed, so a crash between the two
    # is retried on restart instead of losing the alert.
    op.create_table(
        "alert",
        sa.Column("alert_id", sa.Text(), primary_key=True),
        sa.Column("rule", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("location_id", sa.Text(), nullable=False),
        sa.Column("variable", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("station", sa.Text(), nullable=True),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metric", sa.Float(), nullable=False),
        sa.Column("threshold", sa.Float(), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False),
        sa.Column("triggered_by_event_id", sa.Text(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "rule in ('run_change', 'model_spread', 'observation_miss')", name="ck_alert_rule"
        ),
        sa.CheckConstraint("severity in ('warning', 'critical')", name="ck_alert_severity"),
        schema="gold",
    )
    op.create_index("ix_alert_detected_at", "alert", ["detected_at"], schema="gold")
    op.create_index("ix_alert_location", "alert", ["location_id", "detected_at"], schema="gold")


def downgrade() -> None:
    op.drop_index("ix_alert_location", table_name="alert", schema="gold")
    op.drop_index("ix_alert_detected_at", table_name="alert", schema="gold")
    op.drop_table("alert", schema="gold")
