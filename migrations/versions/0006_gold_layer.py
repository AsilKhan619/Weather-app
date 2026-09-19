"""gold layer: forecast_verification, accuracy_daily, model_leaderboard

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Combining daily aggregates into a window is exact, not an approximation:
#   n * mae  = sum(|e|)      so  window MAE  = sum(n * mae) / sum(n)
#   n * rmse^2 = sum(e^2)    so  window RMSE = sqrt(sum(n * rmse^2) / sum(n))
#   n * bias = sum(e)        so  window bias = sum(n * bias) / sum(n)
# The windows end at the latest verified day, not now(): a historical load should
# rank its own most recent 7/30 days, not return nothing.
_LEADERBOARD_VIEW = """
CREATE VIEW gold.model_leaderboard AS
WITH bounds AS (
    SELECT max(valid_date) AS window_end FROM gold.accuracy_daily
),
windows(window_days) AS (VALUES (7), (30)),
agg AS (
    SELECT w.window_days,
           b.window_end,
           a.location_id,
           a.variable,
           a.lead_day,
           a.model,
           sum(a.n)                                    AS n,
           sum(a.n * a.bias) / sum(a.n)                AS bias,
           sum(a.n * a.mae) / sum(a.n)                 AS mae,
           sqrt(sum(a.n * a.rmse * a.rmse) / sum(a.n)) AS rmse
    FROM gold.accuracy_daily a
    CROSS JOIN bounds b
    CROSS JOIN windows w
    WHERE a.valid_date > b.window_end - w.window_days
    GROUP BY w.window_days, b.window_end, a.location_id, a.variable, a.lead_day, a.model
)
SELECT agg.*,
       rank() OVER (
           PARTITION BY window_days, location_id, variable, lead_day ORDER BY mae
       ) AS mae_rank
FROM agg
"""


def upgrade() -> None:
    op.create_table(
        "forecast_verification",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("location_id", sa.Text(), nullable=False),
        sa.Column("init_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("variable", sa.Text(), nullable=False),
        sa.Column("lead_hours", sa.Integer(), nullable=False),
        sa.Column("lead_day", sa.SmallInteger(), nullable=False),
        sa.Column("forecast_value", sa.Float(), nullable=False),
        sa.Column("observed_value", sa.Float(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("obs_offset_seconds", sa.Integer(), nullable=False),
        sa.Column("error", sa.Float(), nullable=False),
        sa.Column("ingestion_mode", sa.Text(), nullable=False),
        sa.Column(
            "computed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("model", "location_id", "init_time", "valid_time", "variable"),
        schema="gold",
    )
    # A day is rebuilt by syncing its valid_time range (upsert what differs, delete
    # what is no longer produced), so that range needs an index.
    op.create_index(
        "ix_forecast_verification_valid_time",
        "forecast_verification",
        ["valid_time"],
        schema="gold",
    )

    op.create_table(
        "accuracy_daily",
        sa.Column("valid_date", sa.Date(), nullable=False),
        sa.Column("location_id", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("variable", sa.Text(), nullable=False),
        sa.Column("lead_day", sa.SmallInteger(), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("bias", sa.Float(), nullable=False),
        sa.Column("mae", sa.Float(), nullable=False),
        sa.Column("rmse", sa.Float(), nullable=False),
        sa.Column(
            "computed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("valid_date", "location_id", "model", "variable", "lead_day"),
        schema="gold",
    )

    op.execute(_LEADERBOARD_VIEW)

    # High-water mark for incremental builds, and a per-day audit of what each
    # build saw (eligible forecasts vs. how many found an observation).
    op.create_table(
        "job_state",
        sa.Column("job", sa.Text(), primary_key=True),
        sa.Column("watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        schema="ops",
    )
    op.create_table(
        "gold_build_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "built_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("run_kind", sa.Text(), nullable=False),
        sa.Column("valid_date", sa.Date(), nullable=False),
        sa.Column("eligible_forecasts", sa.Integer(), nullable=False),
        sa.Column("matched", sa.Integer(), nullable=False),
        sa.CheckConstraint("run_kind in ('incremental', 'full')", name="ck_gold_build_log_kind"),
        schema="ops",
    )
    op.create_index("ix_gold_build_log_valid_date", "gold_build_log", ["valid_date"], schema="ops")


def downgrade() -> None:
    op.drop_index("ix_gold_build_log_valid_date", table_name="gold_build_log", schema="ops")
    op.drop_table("gold_build_log", schema="ops")
    op.drop_table("job_state", schema="ops")
    op.execute("DROP VIEW IF EXISTS gold.model_leaderboard")
    op.drop_table("accuracy_daily", schema="gold")
    op.drop_index(
        "ix_forecast_verification_valid_time", table_name="forecast_verification", schema="gold"
    )
    op.drop_table("forecast_verification", schema="gold")
