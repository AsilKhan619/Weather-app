import math

import pandas as pd
import pytest

from nimbus.gold.verification import (
    aggregate_accuracy,
    lead_day,
    verify_forecasts,
)

STATIONS = pd.DataFrame({"location_id": ["nyc"], "station": ["KJFK"]})


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _forecast(
    valid: str,
    value: float,
    *,
    lead_hours: int = 24,
    variable: str = "temperature_2m",
    location: str = "nyc",
    model: str = "gfs_seamless",
    init: str = "2026-01-01T00:00",
) -> dict[str, object]:
    return {
        "model": model,
        "location_id": location,
        "init_time": _ts(init),
        "valid_time": _ts(valid),
        "variable": variable,
        "value": value,
        "lead_hours": lead_hours,
        "ingestion_mode": "backfill",
    }


def _obs(
    at: str, value: float, *, variable: str = "temperature_2m", station: str = "KJFK"
) -> dict[str, object]:
    return {"station": station, "observed_at": _ts(at), "variable": variable, "value": value}


def _verify(
    forecasts: list[dict[str, object]],
    observations: list[dict[str, object]],
    stations: pd.DataFrame = STATIONS,
) -> pd.DataFrame:
    return verify_forecasts(
        pd.DataFrame(forecasts),
        pd.DataFrame(observations),
        stations,
        tolerance_minutes=30,
        min_lead_hours=1,
    )


def test_error_is_forecast_minus_observed() -> None:
    out = _verify([_forecast("2026-01-02T12:00", 280.0)], [_obs("2026-01-02T12:00", 278.0)])
    assert len(out) == 1
    assert out.loc[0, "error"] == 2.0
    assert out.loc[0, "obs_offset_seconds"] == 0


def test_picks_nearest_observation_within_tolerance() -> None:
    out = _verify(
        [_forecast("2026-01-02T12:00", 280.0)],
        [_obs("2026-01-02T11:20", 270.0), _obs("2026-01-02T12:10", 279.0)],
    )
    assert out.loc[0, "observed_value"] == 279.0
    assert out.loc[0, "obs_offset_seconds"] == 600


def test_observation_before_valid_time_has_negative_offset() -> None:
    out = _verify([_forecast("2026-01-02T12:00", 280.0)], [_obs("2026-01-02T11:51", 279.0)])
    assert out.loc[0, "obs_offset_seconds"] == -540


def test_observation_outside_tolerance_leaves_forecast_unverified() -> None:
    out = _verify([_forecast("2026-01-02T12:00", 280.0)], [_obs("2026-01-02T12:31", 279.0)])
    assert out.empty


def test_nan_observation_is_skipped_for_next_nearest_valid_one() -> None:
    out = _verify(
        [_forecast("2026-01-02T12:00", 280.0)],
        [_obs("2026-01-02T12:00", math.nan), _obs("2026-01-02T12:25", 277.0)],
    )
    assert out.loc[0, "observed_value"] == 277.0


def test_nan_forecast_is_not_verified() -> None:
    out = _verify([_forecast("2026-01-02T12:00", math.nan)], [_obs("2026-01-02T12:00", 278.0)])
    assert out.empty


def test_analysis_step_lead_zero_is_excluded() -> None:
    out = _verify(
        [_forecast("2026-01-02T12:00", 280.0, lead_hours=0)], [_obs("2026-01-02T12:00", 278.0)]
    )
    assert out.empty


def test_match_is_per_station_and_variable() -> None:
    out = _verify(
        [_forecast("2026-01-02T12:00", 280.0)],
        [
            _obs("2026-01-02T12:00", 1.0, station="KLAX"),
            _obs("2026-01-02T12:00", 2.0, variable="dew_point_2m"),
        ],
    )
    assert out.empty


def test_location_without_station_is_dropped() -> None:
    out = _verify(
        [_forecast("2026-01-02T12:00", 280.0, location="nowhere")],
        [_obs("2026-01-02T12:00", 278.0)],
    )
    assert out.empty


def test_each_forecast_row_verified_independently() -> None:
    forecasts = [
        _forecast("2026-01-02T12:00", 280.0, lead_hours=24, init="2026-01-01T12:00"),
        _forecast("2026-01-02T12:00", 285.0, lead_hours=48, init="2025-12-31T12:00"),
        _forecast("2026-01-02T12:00", 281.0, lead_hours=24, model="icon_seamless"),
    ]
    out = _verify(forecasts, [_obs("2026-01-02T12:00", 278.0)])
    assert sorted(out["error"]) == [2.0, 3.0, 7.0]
    assert set(out["lead_day"]) == {1, 2}


def test_empty_inputs_return_empty_frame_with_columns() -> None:
    out = _verify([_forecast("2026-01-02T12:00", 280.0)], [_obs("2026-01-02T12:00", math.nan)])
    assert out.empty
    assert "error" in out.columns


@pytest.mark.parametrize(
    ("hours", "expected"),
    [(1, 1), (23, 1), (24, 1), (25, 2), (48, 2), (49, 3), (168, 7)],
)
def test_lead_day_boundaries(hours: int, expected: int) -> None:
    assert lead_day(pd.Series([hours], dtype="int32")).iloc[0] == expected


def test_aggregate_accuracy_math() -> None:
    forecasts = [
        _forecast("2026-01-02T10:00", 281.0),
        _forecast("2026-01-02T12:00", 277.0),
        _forecast("2026-01-02T14:00", 283.0),
    ]
    observations = [
        _obs("2026-01-02T10:00", 280.0),
        _obs("2026-01-02T12:00", 280.0),
        _obs("2026-01-02T14:00", 280.0),
    ]
    daily = aggregate_accuracy(_verify(forecasts, observations))
    assert len(daily) == 1
    row = daily.iloc[0]
    # errors: +1, -3, +3
    assert row["n"] == 3
    assert row["bias"] == pytest.approx(1 / 3)
    assert row["mae"] == pytest.approx(7 / 3)
    assert row["rmse"] == pytest.approx(math.sqrt((1 + 9 + 9) / 3))


def test_aggregate_groups_by_day_lead_and_model() -> None:
    forecasts = [
        _forecast("2026-01-02T23:00", 281.0, lead_hours=24),
        _forecast("2026-01-03T00:00", 281.0, lead_hours=24),
        _forecast("2026-01-03T00:00", 285.0, lead_hours=48),
    ]
    observations = [_obs("2026-01-02T23:00", 280.0), _obs("2026-01-03T00:00", 280.0)]
    daily = aggregate_accuracy(_verify(forecasts, observations))
    assert list(daily["n"]) == [1, 1, 1]
    assert [d.isoformat() for d in daily["valid_date"]] == [
        "2026-01-02",
        "2026-01-03",
        "2026-01-03",
    ]
    assert list(daily["lead_day"]) == [1, 1, 2]


def test_window_stats_from_daily_aggregates_are_exact() -> None:
    """The leaderboard view combines daily rows as sum(n*mae)/sum(n) etc. That must
    equal computing the statistic over the raw errors."""
    errors_by_day = {"2026-01-02": [1.0, -3.0, 2.0], "2026-01-03": [4.0, -1.0]}
    forecasts, observations = [], []
    for day, errors in errors_by_day.items():
        for hour, err in enumerate(errors):
            valid = f"{day}T{hour:02d}:00"
            forecasts.append(_forecast(valid, 280.0 + err))
            observations.append(_obs(valid, 280.0))
    daily = aggregate_accuracy(_verify(forecasts, observations))

    all_errors = [e for errors in errors_by_day.values() for e in errors]
    n = daily["n"].sum()
    assert n == len(all_errors)
    assert (daily["n"] * daily["bias"]).sum() / n == pytest.approx(sum(all_errors) / n)
    assert (daily["n"] * daily["mae"]).sum() / n == pytest.approx(
        sum(abs(e) for e in all_errors) / n
    )
    rmse = math.sqrt((daily["n"] * daily["rmse"] ** 2).sum() / n)
    assert rmse == pytest.approx(math.sqrt(sum(e * e for e in all_errors) / n))


def test_aggregate_empty_returns_empty_frame() -> None:
    out = aggregate_accuracy(pd.DataFrame())
    assert out.empty
    assert list(out.columns)[:2] == ["valid_date", "location_id"]
