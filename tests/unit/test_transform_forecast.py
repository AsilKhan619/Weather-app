from datetime import UTC, datetime

import pandas as pd
import pytest

from nimbus.common.schemas import ForecastRawPayload
from nimbus.transform.forecast import explode_forecast_payload


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
