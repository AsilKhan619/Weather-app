"""agent: read-only role, ops.agent_sessions, ops.replay_proposals

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-24

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from nimbus.common.settings import get_settings

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SCHEMAS = ("silver", "gold", "ops")


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def upgrade() -> None:
    settings = get_settings()
    role = settings.postgres_readonly_user
    if not role.replace("_", "").isalnum():
        raise ValueError(f"unexpected read-only role name {role!r}")
    password = _quote_literal(settings.postgres_readonly_password)

    # The agent's run_sql connects as this role (ADR 0009). SELECT is its only privilege, every
    # transaction it opens is read-only, and a statement cannot run longer than 10 s - the
    # second and third layers behind the parser's SELECT-only check. Roles are cluster-wide, so
    # the role may already exist (another database, or a re-run): create it only if missing.
    op.execute(
        f"""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN
                CREATE ROLE {role} LOGIN PASSWORD {password};
            END IF;
        END $$
        """
    )
    op.execute(f"ALTER ROLE {role} SET default_transaction_read_only = on")
    op.execute(f"ALTER ROLE {role} SET statement_timeout = '10s'")
    op.execute(f"GRANT CONNECT ON DATABASE {_current_database()} TO {role}")
    for schema in _SCHEMAS:
        op.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
        op.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role}")
        # Tables created later (new migrations, new monthly partitions) are readable too.
        op.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {role}")

    # One row per question the agent answered (or gave up on): the answer, and the full tool
    # trace - every tool call with its input, output excerpt and timing - for the dashboard.
    op.create_table(
        "agent_sessions",
        sa.Column("session_id", sa.Text(), primary_key=True),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("purpose", sa.Text(), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("answer", sa.Text(), nullable=True),
        sa.Column("stop_reason", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("iterations", sa.SmallInteger(), nullable=False),
        sa.Column("tool_calls", sa.SmallInteger(), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("cost_usd", sa.Float(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("trace", postgresql.JSONB(), nullable=False),
        sa.CheckConstraint(
            "stop_reason in ('answered', 'max_iterations', 'token_budget', 'error', 'disabled')",
            name="ck_agent_sessions_stop_reason",
        ),
        schema="ops",
    )
    op.create_index("ix_agent_sessions_started_at", "agent_sessions", ["started_at"], schema="ops")

    # The agent may only *propose* a replay. A human approves or rejects it in the dashboard;
    # approval records the decision and the exact runbook command - it never runs it.
    op.create_table(
        "replay_proposals",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "proposed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("proposed_by", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("consumer_group", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("from_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by", sa.Text(), nullable=True),
        sa.Column("decision_note", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status in ('pending', 'approved', 'rejected')", name="ck_replay_proposals_status"
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (decided_at IS NULL AND decided_by IS NULL)",
            name="ck_replay_proposals_decision",
        ),
        schema="ops",
    )
    op.create_index(
        "ix_replay_proposals_status", "replay_proposals", ["status", "proposed_at"], schema="ops"
    )
    # The new tables are covered by the default privileges above only if they were created by
    # the same owner; grant explicitly so the read-only role can always see them.
    op.execute(f"GRANT SELECT ON ops.agent_sessions, ops.replay_proposals TO {role}")


def _current_database() -> str:
    name = op.get_bind().execute(sa.text("SELECT current_database()")).scalar_one()
    return '"' + str(name).replace('"', '""') + '"'


def downgrade() -> None:
    role = get_settings().postgres_readonly_user
    op.drop_index("ix_replay_proposals_status", table_name="replay_proposals", schema="ops")
    op.drop_table("replay_proposals", schema="ops")
    op.drop_index("ix_agent_sessions_started_at", table_name="agent_sessions", schema="ops")
    op.drop_table("agent_sessions", schema="ops")
    for schema in _SCHEMAS:
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE SELECT ON TABLES FROM {role}"
        )
        op.execute(f"REVOKE SELECT ON ALL TABLES IN SCHEMA {schema} FROM {role}")
        op.execute(f"REVOKE USAGE ON SCHEMA {schema} FROM {role}")
    op.execute(f"REVOKE CONNECT ON DATABASE {_current_database()} FROM {role}")
    # The role itself is cluster-wide and may serve other databases; it is left in place.
