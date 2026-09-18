from datetime import UTC, date, datetime

import pandas as pd
import pytest

from nimbus.common.schemas import ForecastBackfillRawPayload, ForecastRawPayload
from nimbus.transform.forecast import (
    FORECAST_COLUMNS,
    explode_forecast_payload,
    explode_previous_runs_payload,
)


def _payload(hourly: dict[str, list[object]]) -> ForecastRawPayload:
    return ForecastRawPayload(
        model="gfs_seamless",
        location_id="san-francisco",
        run=datetime(2026, 9, 17, 12, 0, tzinfo=UTC),
        api_response={"latitude": 37.6, "longitude": -122.4, "hourly": hourly},
    )


def test_explodes_to_one_row_per_variable_and_hour() -> None:
    payload = _payload(
        {
            "time": ["2026-09-17T12:00", "2026-09-17T13:00"],
            "temperature_2m": [20.0, 21.0],
            "wind_speed_10m": [10.0, 20.0],
        }
    )
    df = explode_forecast_payload(payload, "live")

    assert len(df) == 4  # 2 variables x 2 hours
    assert set(df["variable"]) == {"temperature_2m", "wind_speed_10m"}
    assert list(df.columns) == [
        "model",
        "location_id",
        "init_time",
        "valid_time",
        "variable",
        "value",
        "lead_hours",
        "ingestion_mode",
    ]


def test_converts_units_to_si() -> None:
    payload = _payload(
        {
            "time": ["2026-09-17T12:00"],
            "temperature_2m": [20.0],  # degC
            "dew_point_2m": [10.0],  # degC
            "wind_speed_10m": [36.0],  # km/h
            "pressure_msl": [1013.25],  # hPa
        }
    )
    df = explode_forecast_payload(payload, "live").set_index("variable")["value"]

    assert df["temperature_2m"] == pytest.approx(293.15)  # K
    assert df["dew_point_2m"] == pytest.approx(283.15)  # K
    assert df["wind_speed_10m"] == pytest.approx(10.0)  # m/s
    assert df["pressure_msl"] == pytest.approx(101325.0)  # Pa


def test_missing_values_become_nan_not_dropped() -> None:
    payload = _payload(
        {
            "time": ["2026-09-17T12:00", "2026-09-17T13:00"],
            "temperature_2m": [20.0, None],
        }
    )
    df = explode_forecast_payload(payload, "live")

    assert len(df) == 2
    assert df["value"].isna().sum() == 1
    assert df.loc[df["value"].notna(), "value"].iloc[0] == pytest.approx(293.15)


def test_lead_hours_computed_from_init_and_valid_time() -> None:
    payload = _payload(
        {
            "time": ["2026-09-17T12:00", "2026-09-18T00:00", "2026-09-19T12:00"],
            "temperature_2m": [20.0, 21.0, 22.0],
        }
    )
    df = explode_forecast_payload(payload, "live").sort_values("valid_time")

    assert list(df["lead_hours"]) == [0, 12, 48]


def test_categorical_dtypes_set_explicitly() -> None:
    payload = _payload({"time": ["2026-09-17T12:00"], "temperature_2m": [20.0]})
    df = explode_forecast_payload(payload, "backfill")

    assert isinstance(df["model"].dtype, pd.CategoricalDtype)
    assert isinstance(df["location_id"].dtype, pd.CategoricalDtype)
    assert isinstance(df["variable"].dtype, pd.CategoricalDtype)
    assert isinstance(df["ingestion_mode"].dtype, pd.CategoricalDtype)
    assert df["ingestion_mode"].iloc[0] == "backfill"


def test_malformed_payload_missing_hourly_key_raises() -> None:
    payload = ForecastRawPayload(
        model="gfs_seamless",
        location_id="san-francisco",
        run=datetime(2026, 9, 17, 12, 0, tzinfo=UTC),
        api_response={"latitude": 37.6, "longitude": -122.4},  # no "hourly" key
    )
    with pytest.raises(KeyError):
        explode_forecast_payload(payload, "live")


def _backfill_payload(hourly: dict[str, list[object]]) -> ForecastBackfillRawPayload:
    return ForecastBackfillRawPayload(
        model="gfs_seamless",
        location_id="san-francisco",
        start_date=date(2024, 6, 1),
        end_date=date(2024, 6, 1),
        api_response={"latitude": 37.6, "longitude": -122.4, "hourly": hourly},
    )


def test_backfill_derives_init_time_and_lead_hours_from_the_offset() -> None:
    payload = _backfill_payload(
        {
            "time": ["2024-06-01T12:00", "2024-06-01T13:00"],
            "temperature_2m_previous_day1": [20.0, 21.0],
            "temperature_2m_previous_day3": [19.0, 18.0],
        }
    )
    df = explode_previous_runs_payload(payload)

    assert len(df) == 4  # 2 hours x 2 lead offsets
    day3 = df[df["lead_hours"] == 72].sort_values("valid_time")
    assert list(day3["init_time"]) == [
        pd.Timestamp("2024-05-29T12:00", tz="UTC"),
        pd.Timestamp("2024-05-29T13:00", tz="UTC"),
    ]
    assert set(df["variable"]) == {"temperature_2m"}
    assert (df["ingestion_mode"] == "backfill").all()


def test_backfill_uses_same_columns_and_si_units_as_live() -> None:
    payload = _backfill_payload(
        {
            "time": ["2024-06-01T12:00"],
            "wind_speed_10m_previous_day1": [36.0],  # km/h
            "pressure_msl_previous_day2": [1013.25],  # hPa
        }
    )
    df = explode_previous_runs_payload(payload)

    assert list(df.columns) == FORECAST_COLUMNS
    by_var = df.set_index("variable")["value"]
    assert by_var["wind_speed_10m"] == pytest.approx(10.0)
    assert by_var["pressure_msl"] == pytest.approx(101325.0)


def test_backfill_missing_values_become_nan_not_dropped() -> None:
    payload = _backfill_payload(
        {
            "time": ["2024-06-01T12:00", "2024-06-01T13:00"],
            "temperature_2m_previous_day1": [None, 21.0],
        }
    )
    df = explode_previous_runs_payload(payload)

    assert len(df) == 2
    assert df["value"].isna().sum() == 1


def test_backfill_ignores_non_previous_day_columns() -> None:
    payload = _backfill_payload(
        {
            "time": ["2024-06-01T12:00"],
            "temperature_2m": [99.0],  # not a *_previous_dayN column
            "temperature_2m_previous_day1": [20.0],
        }
    )
    df = explode_previous_runs_payload(payload)

    assert len(df) == 1
    assert df["value"].iloc[0] == pytest.approx(293.15)


def test_backfill_payload_without_previous_day_columns_raises() -> None:
    payload = _backfill_payload({"time": ["2024-06-01T12:00"], "temperature_2m": [20.0]})
    with pytest.raises(KeyError):
        explode_previous_runs_payload(payload)
