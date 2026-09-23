"""Stored SI values -> the units people read (degC, hPa, m/s). One place, shared by the
dashboard and the LLM fact sheets, so a number on a page and a number in a briefing are
always the same number."""

import pandas as pd

# variable -> (display unit, offset, scale): shown = stored * scale + offset
_DISPLAY: dict[str, tuple[str, float, float]] = {
    "temperature_2m": ("degC", -273.15, 1.0),
    "dew_point_2m": ("degC", -273.15, 1.0),
    "wind_speed_10m": ("m/s", 0.0, 1.0),
    "pressure_msl": ("hPa", 0.0, 0.01),
}

VARIABLE_LABELS = {
    "temperature_2m": "Temperature (2 m)",
    "dew_point_2m": "Dew point (2 m)",
    "wind_speed_10m": "Wind speed (10 m)",
    "pressure_msl": "Sea-level pressure",
}


def display_unit(variable: str) -> str:
    return _DISPLAY[variable][0]


def display[ValueT: (float, pd.Series)](variable: str, values: ValueT) -> ValueT:
    """Stored SI value -> display unit."""
    _, offset, scale = _DISPLAY[variable]
    return values * scale + offset


def display_error[ValueT: (float, pd.Series)](variable: str, values: ValueT) -> ValueT:
    """An *error or difference* in display units: a scale applies, an offset does not
    (a 2 K error is a 2 degC error, not -271 degC)."""
    return values * _DISPLAY[variable][2]
