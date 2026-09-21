"""The three anomaly rules and the detector around them, with hand-built runs and an
in-memory stand-in for silver. Each test states the scenario it is about."""

from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pytest

from nimbus.alerts import rules
from nimbus.alerts.detector import ALERT_TOPIC, FORECAST_TOPIC, OBSERVATION_TOPIC, Detector
from nimbus.common.config import AlertsConfig, Location, load_alerts_config
from nimbus.common.events import EventEnvelope
from nimbus.common.schemas import ForecastRawPayload, ObservationRawPayload

CONFIG = load_alerts_config()
INIT = pd.Timestamp("2026-09-20T12:00", tz="UTC")
MODELS = ["ecmwf_ifs025", "gfs_seamless", "icon_seamless"]


def _run(
    offset: float, *, variable: str = "temperature_2m", hours: int = 72, init: Any = INIT
) -> pd.DataFrame:
    """A run whose forecast is 280 K + `offset`, hourly, from init+1h."""
    times = [init + pd.Timedelta(hours=h) for h in range(1, hours + 1)]
    return pd.DataFrame({"variable": variable, "valid_time": times, "value": 280.0 + offset})


# --- rule 1: run-to-run change ----------------------------------------------------------


def test_run_change_alerts_when_the_new_run_moves_the_forecast_beyond_the_threshold() -> None:
    findings = rules.run_change(
        _run(4.0), _run(0.0), init=INIT, previous_init=INIT - pd.Timedelta(hours=6), config=CONFIG
    )

    assert [f.variable for f in findings] == ["temperature_2m"]
    assert findings[0].metric == pytest.approx(4.0)
    assert findings[0].severity == "warning"  # 4.0 >= 3.0 but < 2 x 3.0
    assert findings[0].details["mean_signed_change"] == pytest.approx(4.0)


def test_run_change_is_critical_at_twice_the_threshold() -> None:
    (finding,) = rules.run_change(
        _run(6.5), _run(0.0), init=INIT, previous_init=INIT, config=CONFIG
    )
    assert finding.severity == "critical"


def test_run_change_is_silent_for_an_ordinary_revision() -> None:
    assert (
        rules.run_change(_run(1.0), _run(0.0), init=INIT, previous_init=INIT, config=CONFIG) == []
    )


def test_run_change_uses_absolute_difference_so_opposite_swings_do_not_cancel() -> None:
    new = _run(0.0)
    new["value"] = 280.0 + np.where(np.arange(len(new)) % 2 == 0, 5.0, -5.0)
    (finding,) = rules.run_change(new, _run(0.0), init=INIT, previous_init=INIT, config=CONFIG)

    assert finding.metric == pytest.approx(5.0)
    assert abs(finding.details["mean_signed_change"]) < 0.2


def test_run_change_needs_enough_overlapping_hours() -> None:
    short = _run(10.0, hours=12)
    assert rules.run_change(short, _run(0.0), init=INIT, previous_init=INIT, config=CONFIG) == []


def test_run_change_only_looks_at_the_next_48_hours() -> None:
    new, old = _run(0.0), _run(0.0)
    beyond = new["valid_time"] > INIT + pd.Timedelta(hours=48)
    new.loc[beyond, "value"] += 30.0  # a huge change - but after the horizon
    assert rules.run_change(new, old, init=INIT, previous_init=INIT, config=CONFIG) == []


def test_run_change_ignores_missing_values() -> None:
    new = _run(0.0)
    new.loc[new.index[:10], "value"] = np.nan
    assert rules.run_change(new, _run(0.0), init=INIT, previous_init=INIT, config=CONFIG) == []


def test_run_change_checks_each_variable_against_its_own_threshold() -> None:
    wind_new = _run(0.0, variable="wind_speed_10m")
    wind_new["value"] = 12.0  # 12 m/s vs 8 m/s: +4 exceeds wind's 3.0
    wind_old = _run(0.0, variable="wind_speed_10m")
    wind_old["value"] = 8.0
    temp = _run(2.0)  # +2 K stays under temperature's 3.0
    findings = rules.run_change(
        pd.concat([wind_new, temp]), pd.concat([wind_old, _run(0.0)]),
        init=INIT, previous_init=INIT, config=CONFIG,
    )  # fmt: skip
    assert [f.variable for f in findings] == ["wind_speed_10m"]


# --- rule 2: model spread ---------------------------------------------------------------


def test_spread_alerts_when_the_models_disagree() -> None:
    runs = {"ecmwf_ifs025": _run(0.0), "gfs_seamless": _run(3.0), "icon_seamless": _run(6.0)}
    (finding,) = rules.model_spread(runs, init=INIT, config=CONFIG)

    assert finding.metric == pytest.approx(6.0)  # max - min every hour
    assert finding.details["models"] == sorted(runs)
    assert finding.details["model_means"]["icon_seamless"] == pytest.approx(286.0)


def test_spread_is_silent_when_the_models_agree() -> None:
    runs = {"a": _run(0.0), "b": _run(0.5), "c": _run(1.0)}
    assert rules.model_spread(runs, init=INIT, config=CONFIG) == []


def test_spread_needs_two_models() -> None:
    assert rules.model_spread({"a": _run(0.0)}, init=INIT, config=CONFIG) == []


def test_spread_only_counts_hours_where_two_models_have_data() -> None:
    a, b = _run(0.0), _run(9.0, hours=10)  # b only covers 10 hours: too little overlap
    assert rules.model_spread({"a": a, "b": b}, init=INIT, config=CONFIG) == []


# --- rule 3: an observation misses the forecast -----------------------------------------


def test_observation_miss_compares_against_the_mean_of_the_models() -> None:
    finding = rules.observation_miss("temperature_2m", 290.0, {"a": 282.0, "b": 284.0}, CONFIG)

    assert finding is not None
    assert finding.metric == pytest.approx(7.0)  # |290 - 283|
    assert finding.details["signed_error"] == pytest.approx(-7.0)  # forecast - observed
    assert finding.severity == "warning"


def test_observation_within_tolerance_is_silent() -> None:
    assert rules.observation_miss("temperature_2m", 285.0, {"a": 283.0}, CONFIG) is None


def test_observation_miss_without_a_forecast_or_value_is_silent() -> None:
    assert rules.observation_miss("temperature_2m", 290.0, {}, CONFIG) is None
    assert rules.observation_miss("temperature_2m", float("nan"), {"a": 280.0}, CONFIG) is None
    assert rules.observation_miss("unknown_variable", 290.0, {"a": 280.0}, CONFIG) is None


def test_classify_boundaries() -> None:
    assert rules.classify(2.99, 3.0, 2.0) is None
    assert rules.classify(3.0, 3.0, 2.0) == "warning"
    assert rules.classify(5.99, 3.0, 2.0) == "warning"
    assert rules.classify(6.0, 3.0, 2.0) == "critical"


# --- the detector, with an in-memory silver -----------------------------------------------


class FakeSilver:
    """Just enough of silver for the detector: a previous run per (model, location), the
    other models' latest runs, and the short-range forecasts for an hour."""

    def __init__(self) -> None:
        self.previous: dict[tuple[str, str], tuple[datetime, pd.DataFrame]] = {}
        self.latest: dict[tuple[str, str], tuple[datetime, pd.DataFrame]] = {}
        self.at: dict[tuple[str, str], dict[str, float]] = {}

    def previous_run(self, model: str, location_id: str, init: datetime, hours: int) -> Any:
        return self.previous.get((model, location_id))

    def latest_run(
        self, model: str, location_id: str, init: datetime, hours: int, cadence_hours: int
    ) -> Any:
        return self.latest.get((model, location_id))

    def forecasts_at(
        self, location_id: str, variable: str, at: datetime, max_lead_hours: int
    ) -> dict[str, float]:
        return self.at.get((location_id, variable), {})


def _location() -> Location:
    return Location(
        id="san-francisco", name="SF", climate="coastal", latitude=37.6, longitude=-122.4,
        elevation_m=4, timezone="America/Los_Angeles", station="KSFO",
    )  # fmt: skip


def _detector(silver: FakeSilver, config: AlertsConfig = CONFIG) -> Detector:
    return Detector(silver, config, [_location()], MODELS, 6)


def _forecast_message(
    *, temp_c: float, mode: str = "live", model: str = "gfs_seamless", init: datetime | None = None
) -> bytes:
    init = init or INIT.to_pydatetime()
    times = [(init + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(1, 73)]
    hourly = {
        "time": times,
        "temperature_2m": [temp_c] * 72,
        "dew_point_2m": [10.0] * 72,
        "wind_speed_10m": [18.0] * 72,
        "pressure_msl": [1013.0] * 72,
    }
    envelope = EventEnvelope[ForecastRawPayload](
        event_id=f"fc-{model}-{init.isoformat()}-{temp_c}",
        source="forecast_producer",
        event_type="forecast.raw",
        produced_at=datetime(2026, 9, 20, tzinfo=UTC),
        ingestion_mode=mode,
        payload=ForecastRawPayload(
            model=model,
            location_id="san-francisco",
            run=init,
            api_response={"hourly": hourly},
        ),
    )
    return envelope.model_dump_json().encode()


def _observation_message(*, temp_c: float, mode: str = "live") -> bytes:
    envelope = EventEnvelope[ObservationRawPayload](
        event_id="obs-1",
        source="observation_producer",
        event_type="observation.raw",
        produced_at=datetime(2026, 9, 20, tzinfo=UTC),
        ingestion_mode=mode,
        payload=ObservationRawPayload(
            station="KSFO",
            observed_at=datetime(2026, 9, 20, 17, 53, tzinfo=UTC),
            api_response={"temp": temp_c, "dewp": 10.0, "wspd": 5, "slp": 1013.0, "rawOb": "METAR"},
        ),
    )
    return envelope.model_dump_json().encode()


def test_detector_alerts_on_a_run_that_jumps_against_the_previous_one() -> None:
    silver = FakeSilver()
    previous = _run(0.0)
    previous["value"] = 293.15  # 20 degC
    silver.previous[("gfs_seamless", "san-francisco")] = (
        (INIT - pd.Timedelta(hours=6)).to_pydatetime(),
        previous,
    )

    alerts = _detector(silver).detect(FORECAST_TOPIC, _forecast_message(temp_c=27.0))  # +7 K

    (alert,) = alerts
    assert (alert.rule, alert.severity, alert.variable) == (
        "run_change",
        "critical",
        "temperature_2m",
    )
    assert alert.model == "gfs_seamless" and alert.location_id == "san-francisco"
    assert alert.metric == pytest.approx(7.0)
    assert alert.event_time == INIT.to_pydatetime()
    assert alert.triggered_by_event_id.startswith("fc-gfs_seamless")


def test_the_same_event_always_yields_the_same_alert_id() -> None:
    silver = FakeSilver()
    previous = _run(0.0)
    previous["value"] = 293.15
    silver.previous[("gfs_seamless", "san-francisco")] = (INIT.to_pydatetime(), previous)
    detector = _detector(silver)

    first = detector.detect(FORECAST_TOPIC, _forecast_message(temp_c=27.0))
    again = detector.detect(FORECAST_TOPIC, _forecast_message(temp_c=27.0))

    assert [a.alert_id for a in first] == [a.alert_id for a in again]
    assert len({a.alert_id for a in first}) == len(first)


def test_detector_alerts_on_model_spread_using_the_other_models_latest_runs() -> None:
    silver = FakeSilver()
    for model, offset in (("ecmwf_ifs025", 0.0), ("icon_seamless", 9.0)):
        silver.latest[(model, "san-francisco")] = (INIT.to_pydatetime(), _run(offset))

    alerts = _detector(silver).detect(FORECAST_TOPIC, _forecast_message(temp_c=6.85))  # 280 K

    spread = [a for a in alerts if a.rule == "model_spread"]
    assert [a.variable for a in spread] == ["temperature_2m"]
    assert spread[0].model is None
    assert spread[0].details["models"] == ["ecmwf_ifs025", "gfs_seamless", "icon_seamless"]


def test_spread_alert_id_is_shared_by_every_model_of_the_same_cycle() -> None:
    """Each of the three models' runs triggers an evaluation; they must not each
    produce their own alert for the same disagreement."""
    silver = FakeSilver()
    for model, offset in (("ecmwf_ifs025", 0.0), ("icon_seamless", 9.0)):
        silver.latest[(model, "san-francisco")] = (INIT.to_pydatetime(), _run(offset))
    detector = _detector(silver)

    via_gfs = detector.detect(FORECAST_TOPIC, _forecast_message(temp_c=6.85))
    silver.latest[("gfs_seamless", "san-francisco")] = (INIT.to_pydatetime(), _run(0.0))
    del silver.latest[("ecmwf_ifs025", "san-francisco")]
    via_ecmwf = detector.detect(
        FORECAST_TOPIC, _forecast_message(temp_c=6.85, model="ecmwf_ifs025")
    )

    ids = {a.alert_id for a in via_gfs if a.rule == "model_spread"}
    assert ids and ids == {a.alert_id for a in via_ecmwf if a.rule == "model_spread"}


def test_detector_alerts_on_an_observation_that_misses_the_forecast() -> None:
    silver = FakeSilver()
    silver.at[("san-francisco", "temperature_2m")] = {
        "gfs_seamless": 288.15,
        "icon_seamless": 289.15,
    }

    alerts = _detector(silver).detect(OBSERVATION_TOPIC, _observation_message(temp_c=25.0))

    (alert,) = alerts  # 298.15 vs mean 288.65 = 9.5 K; the other variables have no forecast
    assert (alert.rule, alert.station, alert.variable) == (
        "observation_miss", "KSFO", "temperature_2m",
    )  # fmt: skip
    assert alert.metric == pytest.approx(9.5)
    assert alert.severity == "warning"


def test_backfilled_history_never_alerts() -> None:
    silver = FakeSilver()
    previous = _run(0.0)
    previous["value"] = 293.15
    silver.previous[("gfs_seamless", "san-francisco")] = (INIT.to_pydatetime(), previous)
    silver.at[("san-francisco", "temperature_2m")] = {"gfs_seamless": 280.0}
    detector = _detector(silver)

    assert detector.detect(FORECAST_TOPIC, _forecast_message(temp_c=40.0, mode="backfill")) == []
    assert (
        detector.detect(OBSERVATION_TOPIC, _observation_message(temp_c=40.0, mode="backfill")) == []
    )


def test_an_implausible_value_that_the_silver_gate_rejects_does_not_alert() -> None:
    silver = FakeSilver()
    silver.at[("san-francisco", "temperature_2m")] = {"gfs_seamless": 280.0}
    assert _detector(silver).detect(OBSERVATION_TOPIC, _observation_message(temp_c=500.0)) == []


def test_an_observation_from_an_unmapped_station_is_ignored() -> None:
    detector = Detector(FakeSilver(), CONFIG, [], MODELS, 6)
    assert detector.detect(OBSERVATION_TOPIC, _observation_message(temp_c=25.0)) == []


def test_other_topics_are_ignored() -> None:
    assert _detector(FakeSilver()).detect(ALERT_TOPIC, b"{}") == []
