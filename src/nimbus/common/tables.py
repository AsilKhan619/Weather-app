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
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="silver",
)

observation_table = sa.Table(
    "observation",
    metadata,
    sa.Column("station", sa.Text, primary_key=True),
    sa.Column("observed_at", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("variable", sa.Text, primary_key=True),
    sa.Column("value", sa.Float),
    sa.Column("raw_text", sa.Text, nullable=False),
    sa.Column("is_corrected", sa.Boolean, nullable=False),
    sa.Column("ingestion_mode", sa.Text, nullable=False),
    sa.Column("source_event_id", sa.Text, nullable=False),
    sa.Column("inserted_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="silver",
)

dim_location_table = sa.Table(
    "dim_location",
    metadata,
    sa.Column("location_id", sa.Text, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("climate", sa.Text, nullable=False),
    sa.Column("latitude", sa.Float, nullable=False),
    sa.Column("longitude", sa.Float, nullable=False),
    sa.Column("elevation_m", sa.Float, nullable=False),
    sa.Column("timezone", sa.Text, nullable=False),
    sa.Column("station", sa.Text, nullable=False),
    schema="silver",
)

dim_model_table = sa.Table(
    "dim_model",
    metadata,
    sa.Column("model_id", sa.Text, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    schema="silver",
)

dim_variable_table = sa.Table(
    "dim_variable",
    metadata,
    sa.Column("variable", sa.Text, primary_key=True),
    sa.Column("unit", sa.Text, nullable=False),
    sa.Column("description", sa.Text, nullable=False),
    schema="silver",
)

forecast_verification_table = sa.Table(
    "forecast_verification",
    metadata,
    sa.Column("model", sa.Text, primary_key=True),
    sa.Column("location_id", sa.Text, primary_key=True),
    sa.Column("init_time", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("valid_time", sa.DateTime(timezone=True), primary_key=True),
    sa.Column("variable", sa.Text, primary_key=True),
    sa.Column("lead_hours", sa.Integer, nullable=False),
    sa.Column("lead_day", sa.SmallInteger, nullable=False),
    sa.Column("forecast_value", sa.Float, nullable=False),
    sa.Column("observed_value", sa.Float, nullable=False),
    sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("obs_offset_seconds", sa.Integer, nullable=False),
    sa.Column("error", sa.Float, nullable=False),
    sa.Column("ingestion_mode", sa.Text, nullable=False),
    sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="gold",
)

accuracy_daily_table = sa.Table(
    "accuracy_daily",
    metadata,
    sa.Column("valid_date", sa.Date, primary_key=True),
    sa.Column("location_id", sa.Text, primary_key=True),
    sa.Column("model", sa.Text, primary_key=True),
    sa.Column("variable", sa.Text, primary_key=True),
    sa.Column("lead_day", sa.SmallInteger, primary_key=True),
    sa.Column("n", sa.Integer, nullable=False),
    sa.Column("bias", sa.Float, nullable=False),
    sa.Column("mae", sa.Float, nullable=False),
    sa.Column("rmse", sa.Float, nullable=False),
    sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="gold",
)

job_state_table = sa.Table(
    "job_state",
    metadata,
    sa.Column("job", sa.Text, primary_key=True),
    sa.Column("watermark", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    schema="ops",
)

gold_build_log_table = sa.Table(
    "gold_build_log",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("built_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column("run_kind", sa.Text, nullable=False),
    sa.Column("valid_date", sa.Date, nullable=False),
    sa.Column("eligible_forecasts", sa.Integer, nullable=False),
    sa.Column("matched", sa.Integer, nullable=False),
    schema="ops",
)

quality_results_table = sa.Table(
    "quality_results",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("checked_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    sa.Column("context", sa.Text, nullable=False),
    sa.Column("table_name", sa.Text, nullable=False),
    sa.Column("check_name", sa.Text, nullable=False),
    sa.Column("severity", sa.Text, nullable=False),
    sa.Column("subject", sa.Text),
    sa.Column("rows_checked", sa.BigInteger, nullable=False),
    sa.Column("rows_failed", sa.BigInteger, nullable=False),
    sa.Column("passed", sa.Boolean, nullable=False),
    sa.Column("detail", sa.Text),
    schema="ops",
)
