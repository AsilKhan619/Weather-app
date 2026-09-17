"""SQLAlchemy Core Table objects mirroring the Alembic migrations (brief
section 3/7: Core + Alembic, no ORM). Migrations own the DDL; these describe
structure for query-building - keep the two in sync by hand."""

import sqlalchemy as sa

metadata = sa.MetaData()

forecast_table = sa.Table(
    "forecast",
    metadata,
    sa.Column("model", sa.Text, primary_key=True),
    sa.Column("location_id", sa.Text, primary_key=True),
    sa.Column("init_time", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("valid_time", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("variable", sa.Text, primary_key=True),
    sa.Column("value", sa.Float),
    sa.Column("lead_hours", sa.Integer, nullable=False),
    sa.Column("ingestion_mode", sa.Text, nullable=False),
    sa.Column("source_event_id", sa.Text, nullable=False),
    sa.Column("inserted_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="silver",
)
