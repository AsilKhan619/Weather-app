"""Pure, unit-tested transform: one raw Open-Meteo payload -> tidy SI-unit
rows (brief section 7). No `iterrows`, explicit dtypes, categoricals for
low-cardinality columns."""

from collections.abc import Callable

import pandas as pd

from nimbus.common.events import IngestionMode
from nimbus.common.schemas import ForecastRawPayload

# Open-Meteo does not return SI units (verified live, ADR 0001): temperature/
# dew point in Celsius, wind speed in km/h, pressure in hPa.
_UNIT_CONVERSIONS: dict[str, Callable[[pd.Series], pd.Series]] = {
    "temperature_2m": lambda s: s + 273.15,  # degC -> K
    "dew_point_2m": lambda s: s + 273.15,  # degC -> K
    "wind_speed_10m": lambda s: s / 3.6,  # km/h -> m/s
    "pressure_msl": lambda s: s * 100.0,  # hPa -> Pa
}

FORECAST_COLUMNS = [
    "model",
    "location_id",
    "init_time",
    "valid_time",
    "variable",
    "value",
    "lead_hours",
    "ingestion_mode",
]


def explode_forecast_payload(
    payload: ForecastRawPayload, ingestion_mode: IngestionMode
) -> pd.DataFrame:
    """One row per (variable, valid_time), SI units, with lead_hours."""
    hourly = payload.api_response["hourly"]
    times = pd.to_datetime(list(hourly["time"]), utc=True)
    variable_columns = [column for column in hourly if column != "time"]

    wide = pd.DataFrame({"valid_time": times})
    for column in variable_columns:
        values = pd.Series(hourly[column], dtype="float64")
        convert = _UNIT_CONVERSIONS.get(column)
        wide[column] = convert(values) if convert else values

    tidy = wide.melt(id_vars="valid_time", var_name="variable", value_name="value")
    tidy["model"] = payload.model
    tidy["location_id"] = payload.location_id
    tidy["init_time"] = pd.Timestamp(payload.run)
    tidy["ingestion_mode"] = ingestion_mode
    tidy["lead_hours"] = (
        (tidy["valid_time"] - tidy["init_time"]).dt.total_seconds() // 3600
    ).astype("int32")

    tidy = tidy.astype(
        {
            "model": "category",
            "location_id": "category",
            "variable": "category",
            "ingestion_mode": "category",
        }
    )
    return tidy[FORECAST_COLUMNS]
