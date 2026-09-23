"""Dashboard v1 against a real Postgres: the query layer returns what the gold layer holds
(in display units), and each Streamlit page renders without error - on an empty database
(every page must say so, not crash) and on a seeded one. Kafka is not needed: the health
page must survive its absence."""

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from sqlalchemy import Engine, text
from streamlit.testing.v1 import AppTest

from nimbus.common.config import GoldConfig, load_locations
from nimbus.common.db import make_engine
from nimbus.common.settings import Settings, get_settings
from nimbus.dashboard import queries
from nimbus.gold.build import build_gold
from nimbus.jobs.load_dimensions import load_dimensions
from nimbus.streaming.forecast_silver import upsert_forecast_rows
from nimbus.streaming.observation_silver import upsert_observation_rows

pytestmark = pytest.mark.integration

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"
CONFIG = GoldConfig(observation_match_tolerance_minutes=30, min_lead_hours=1, lookback_hours=0)
FIRST = date(2026, 1, 2)
DAYS = 10
PLACES = load_locations()[:2]
# model -> forecast error (K) at each lead day: ICON is better and both degrade with lead
ERRORS = {"icon_seamless": {1: 1.0, 3: 2.0}, "gfs_seamless": {1: 3.0, 3: 5.0}}


def _ts(day: date, hour: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def _clean(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE silver.forecast, silver.observation, gold.forecast_verification, "
                "gold.accuracy_daily, gold.alert, ops.gold_build_log, ops.job_state, "
                "ops.quality_results, ops.ingestion_runs, ops.reconciliation_results, "
                "gold.briefing, ops.llm_calls"
            )
        )


def _seed(engine: Engine) -> None:
    for offset in range(DAYS):
        day = FIRST + timedelta(days=offset)
        valid = [_ts(day, h) for h in (0, 6, 12, 18)]
        for place in PLACES:
            upsert_observation_rows(
                engine,
                pd.DataFrame(
                    {
                        "station": place.station,
                        "observed_at": valid,
                        "variable": "temperature_2m",
                        "value": 283.15,  # 10 degC
                        "raw_text": "METAR",
                        "is_corrected": False,
                        "ingestion_mode": "backfill",
                        "source_event_id": f"obs-{place.id}-{day}",
                    }
                ),
            )
            for model, by_lead in ERRORS.items():
                for lead, error in by_lead.items():
                    upsert_forecast_rows(
                        engine,
                        pd.DataFrame(
                            {
                                "model": model,
                                "location_id": place.id,
                                "init_time": [t - timedelta(days=lead) for t in valid],
                                "valid_time": valid,
                                "variable": "temperature_2m",
                                "value": 283.15 + error,
                                "lead_hours": lead * 24,
                                "ingestion_mode": "backfill",
                                "source_event_id": f"fc-{model}-{place.id}-{day}-{lead}",
                            }
                        ),
                    )


@pytest.fixture
def empty(pg_settings: Settings) -> Iterator[Engine]:
    engine = make_engine(pg_settings)
    _clean(engine)
    load_dimensions(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def seeded(empty: Engine) -> Iterator[Engine]:
    _seed(empty)
    build_gold(empty, config=CONFIG)
    yield empty
    _clean(empty)


@pytest.fixture
def app_env(pg_settings: Settings, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point the dashboard at the test database (and at a Kafka that is not there)."""
    for key, value in {
        "POSTGRES_HOST": pg_settings.postgres_host,
        "POSTGRES_PORT": str(pg_settings.postgres_port),
        "POSTGRES_DB": pg_settings.postgres_db,
        "POSTGRES_USER": pg_settings.postgres_user,
        "POSTGRES_PASSWORD": pg_settings.postgres_password,
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    st.cache_resource.clear()
    st.cache_data.clear()
    yield
    get_settings.cache_clear()
    st.cache_resource.clear()
    st.cache_data.clear()


# --- the query layer ------------------------------------------------------------------------


def test_empty_gold_layer_reads_as_empty_not_as_an_error(empty: Engine) -> None:
    assert queries.verified_range(empty) is None
    assert queries.leaderboard(empty, "temperature_2m", 7, 1, None).empty
    assert queries.error_by_lead(empty, "temperature_2m", 30, None).empty
    assert queries.recent_alerts(empty).empty
    assert queries.latest_reconciliation(empty).empty
    assert len(queries.locations(empty)) == len(load_locations())


def test_leaderboard_ranks_by_mae_in_display_units(seeded: Engine) -> None:
    board = queries.leaderboard(seeded, "temperature_2m", 30, 1, None)

    assert list(board["model"]) == ["icon_seamless", "gfs_seamless"]
    assert list(board["rank"]) == [1, 2]
    assert board["mae"].tolist() == pytest.approx([1.0, 3.0])  # K error == degC error
    assert board["bias"].tolist() == pytest.approx([1.0, 3.0])
    assert board["n"].tolist() == [DAYS * 4 * len(PLACES)] * 2


def test_leaderboard_window_and_location_filters(seeded: Engine) -> None:
    week = queries.leaderboard(seeded, "temperature_2m", 7, 1, PLACES[0].id)

    assert week["n"].tolist() == [7 * 4] * 2  # 7 days x 4 hours, one location
    assert queries.leaderboard(seeded, "temperature_2m", 7, 1, "nowhere").empty


def test_error_grows_with_lead_time(seeded: Engine) -> None:
    curve = queries.error_by_lead(seeded, "temperature_2m", 30, None)

    icon = curve[curve["model"] == "icon_seamless"].set_index("lead_day")["mae"]
    assert icon.loc[3] > icon.loc[1]
    assert icon.tolist() == pytest.approx([1.0, 2.0])


def test_best_model_by_location_and_its_margin(seeded: Engine) -> None:
    best = queries.best_model_by_location(seeded, "temperature_2m", 30, 3)

    assert set(best["best_model"]) == {"icon_seamless"}
    assert len(best) == len(PLACES)
    assert best["margin"].tolist() == pytest.approx([3.0] * len(PLACES))  # 5.0 - 2.0


def test_forecast_vs_actual_converts_kelvin_to_celsius(seeded: Engine) -> None:
    place = PLACES[0]
    points = queries.forecast_vs_actual(
        seeded, place.id, "temperature_2m", 1, FIRST, FIRST + timedelta(days=1)
    )

    assert set(points["model"]) == set(ERRORS)
    assert len(points) == 2 * 2 * 4  # 2 models x 2 days x 4 hours
    assert points["observed"].tolist() == pytest.approx([10.0] * len(points))
    icon = points[points["model"] == "icon_seamless"]
    assert icon["forecast"].tolist() == pytest.approx([11.0] * len(icon))
    assert points["valid_time"].min() >= pd.Timestamp(FIRST, tz="UTC")


def test_display_units() -> None:
    assert queries.display("pressure_msl", pd.Series([101325.0])).iloc[0] == pytest.approx(1013.25)
    # an error scales but must not be offset: a 2 K error is 2 degC, not -271
    assert queries.display_error("temperature_2m", pd.Series([2.0])).iloc[0] == 2.0
    assert queries.display_error("pressure_msl", pd.Series([250.0])).iloc[0] == pytest.approx(2.5)


def test_health_queries(seeded: Engine) -> None:
    with seeded.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.ingestion_runs (source, ingestion_mode, status, started_at, "
                "finished_at, messages_produced, messages_failed) VALUES "
                "('forecast_producer', 'live', 'success', now(), now(), 75, 0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO ops.quality_results (context, table_name, check_name, severity, "
                "rows_checked, rows_failed, passed) VALUES "
                "('quality-suite', 'silver.observation', 'hard_range', 'blocking', 100, 3, false), "
                "('quality-suite', 'silver.observation', 'all_checks', 'blocking', 100, 3, false)"
            )
        )

    throughput = queries.hourly_throughput(seeded, 24)
    runs = queries.ingestion_runs(seeded)
    failures = queries.quality_failures(seeded)
    summary = queries.quality_summary(seeded)

    assert set(throughput["table_name"]) == {"forecast", "observation"}
    assert runs.iloc[0]["source"] == "forecast_producer" and runs.iloc[0]["produced"] == 75
    assert failures["check_name"].tolist() == ["hard_range"]  # the summary row is left out
    silver = summary[summary["table_name"] == "silver.observation"].iloc[0]
    assert silver["failing_runs"] == 1 and silver["rows_failed"] == 3


# --- the pages ------------------------------------------------------------------------------

PAGES = [
    "views/health.py",
    "views/forecast_vs_actual.py",
    "views/accuracy.py",
    "views/lineage.py",
    "views/briefings.py",
    "views/llm_usage.py",
]


def _run(page: str) -> AppTest:
    at = AppTest.from_file(str(APP), default_timeout=90)
    at.run()
    assert not at.exception, f"app shell: {at.exception}"
    if page != PAGES[0]:
        at.switch_page(page)
        at.run()
    return at


@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders_on_an_empty_database(page: str, empty: Engine, app_env: None) -> None:
    at = _run(page)

    assert not at.exception, at.exception
    assert at.title  # rendered its heading rather than dying before it


def test_health_page_survives_kafka_being_down(seeded: Engine, app_env: None) -> None:
    at = _run("views/health.py")

    assert not at.exception, at.exception
    assert any("Kafka is not reachable" in w.value for w in at.warning)
    assert [m.label for m in at.metric][:2] == ["Alerts (24 h)", "DLQ messages"]
    assert at.metric[1].value == "n/a"


def test_accuracy_page_shows_the_leaderboard_and_the_lead_curve(
    seeded: Engine, app_env: None
) -> None:
    at = _run("views/accuracy.py")

    assert not at.exception, at.exception
    tables = [d.value for d in at.dataframe]
    assert len(tables) >= 2
    assert len(at.get("vega_lite_chart")) == 2  # error versus lead time, and locations won
    assert "icon_seamless" in str(tables[0])  # ranked first in the leaderboard
    assert "icon_seamless" in str(tables[1]) or "icon_seamless" in str(tables[0])


def test_forecast_vs_actual_page_charts_the_selection(seeded: Engine, app_env: None) -> None:
    at = _run("views/forecast_vs_actual.py")
    at.selectbox[0].select(PLACES[0].name).run()  # the default (first by name) has no data

    assert not at.exception, at.exception
    assert at.subheader and "Error over this selection" in [s.value for s in at.subheader]
    assert len(at.dataframe) == 1  # the per-model error table
    assert len(at.get("vega_lite_chart")) == 1  # the forecast-vs-observed lines
    assert set(at.dataframe[0].value["model"]) == {"icon_seamless", "gfs_seamless"}


def test_lineage_page_asks_for_an_event_and_reports_an_unknown_one(
    empty: Engine, app_env: None
) -> None:
    at = _run("views/lineage.py")
    assert not at.exception, at.exception

    at.text_input(key="event_id").set_value("no-such-event").run()

    assert not at.exception, at.exception
    assert any("Not found" in e.value for e in at.error)


# --- Phase 5 pages ------------------------------------------------------------------------


def _briefed(engine: Engine) -> None:
    """One grounded and one flagged briefing, from the deterministic fake client."""
    from nimbus.common.config import load_llm_config
    from nimbus.llm.briefings import generate_briefing
    from nimbus.llm.client import FakeBriefingClient

    as_of = _ts(FIRST + timedelta(days=DAYS - 3), 0)
    for place, client in (
        (PLACES[0], FakeBriefingClient()),
        (PLACES[1], FakeBriefingClient(invent_number=True)),
    ):
        generate_briefing(
            engine, place, as_of, client=client, config=load_llm_config(), producer=None,
            model="fake-briefing-model",
        )  # fmt: skip


def test_briefing_queries(seeded: Engine) -> None:
    _briefed(seeded)

    latest = queries.latest_briefings(seeded)
    counts = queries.briefing_counts(seeded).iloc[0]
    usage = queries.llm_usage_by_outcome(seeded)

    assert len(latest) == 2 and set(latest["grounding_passed"]) == {True, False}
    assert (counts["briefings"], counts["grounded"], counts["flagged"]) == (2, 1, 1)
    assert set(usage["outcome"]) == {"success", "grounding_failed"}
    assert usage["cost_usd"].sum() == 0.0  # the fake model has no price


def test_briefings_page_shows_a_briefing_and_its_fact_sheet(seeded: Engine, app_env: None) -> None:
    _briefed(seeded)
    at = _run("views/briefings.py")

    assert not at.exception, at.exception
    assert [m.label for m in at.metric] == [
        "Briefings (7 d)", "Grounded", "Flagged (never published)", "Published",
    ]  # fmt: skip
    assert at.metric[2].value == "1"
    assert at.expander and "Fact sheet" in at.expander[0].label

    flagged = queries.latest_briefings(seeded)
    flagged_name = flagged.loc[~flagged["grounding_passed"], "location"].iloc[0]
    at.selectbox[0].select(flagged_name).run()
    assert any("Flagged" in e.value for e in at.error)


def test_llm_usage_page(seeded: Engine, app_env: None) -> None:
    _briefed(seeded)
    at = _run("views/llm_usage.py")

    assert not at.exception, at.exception
    labels = [m.label for m in at.metric]
    assert labels[:3] == ["Cost (30 d, estimated)", "API calls", "Cache hit rate"]
    assert at.metric[1].value == "2"
