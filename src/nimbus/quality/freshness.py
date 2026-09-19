"""Freshness monitoring (brief section 8): flag any station or source with no new
data within its expected interval. Always a warning - a quiet source needs
attention, but nothing about the data already stored is wrong."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, text

from nimbus.common.config import QualityConfig, load_quality_config
from nimbus.quality.runner import CheckResult

# Live ingestion sources and the config attribute holding their expected interval.
_SOURCES = {
    "observation_producer": "observation_max_age_hours",
    "forecast_producer": "forecast_max_run_age_hours",
}


def _age(now: datetime, then: datetime | None) -> timedelta | None:
    return None if then is None else now - then


def _result(table: str, subject: str, age: timedelta | None, limit_hours: float) -> CheckResult:
    if age is None:
        return CheckResult(table, "freshness", "warning", 1, 1, subject, "no data yet")
    hours = age.total_seconds() / 3600
    stale = hours > limit_hours
    detail = f"newest data is {hours:.1f} h old (limit {limit_hours:g} h)"
    return CheckResult(table, "freshness", "warning", 1, int(stale), subject, detail)


def check_freshness(
    engine: Engine, config: QualityConfig | None = None, *, now: datetime | None = None
) -> list[CheckResult]:
    """One result per configured station (newest observation) and per live source
    (last successful ingestion run)."""
    config = config or load_quality_config()
    now = now or datetime.now(UTC)
    results: list[CheckResult] = []

    with engine.connect() as conn:
        # One index probe per station; stations that never reported still appear.
        stations = conn.execute(
            text(
                "SELECT l.station, "
                "(SELECT max(o.observed_at) FROM silver.observation o WHERE o.station = l.station) "
                "FROM silver.dim_location l ORDER BY l.station"
            )
        ).all()
        for station, newest in stations:
            age = _age(now, newest)
            results.append(
                _result("silver.observation", station, age, config.observation_max_age_hours)
            )

        for source, limit_attr in _SOURCES.items():
            newest = conn.execute(
                text(
                    "SELECT max(finished_at) FROM ops.ingestion_runs "
                    "WHERE source = :source AND status = 'success'"
                ),
                {"source": source},
            ).scalar()
            results.append(
                _result(
                    "ops.ingestion_runs", source, _age(now, newest), getattr(config, limit_attr)
                )
            )
    return results
