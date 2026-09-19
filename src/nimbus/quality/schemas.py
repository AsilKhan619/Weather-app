"""pandera schemas for every DataFrame load (brief section 8, ADR 0005).

Each table has two schemas, and which one a check lives in *is* its severity:

- `blocking`: a failing row must not be loaded. Structural checks (non-null keys,
  timezone-aware times, finite values, consistent derived columns) and the *hard*
  physical limits (a temperature of 500 K is a data error, not weather).
- `warning`: the row is kept and flagged. The plausible range (-90..60 degC for air
  temperature) - unusual weather is real, so it must not be discarded.

The natural-key uniqueness check is optional (`unique=True`): a silver batch may
legitimately hold the same key twice (overlapping backfill windows, an original
and its correction) and is deduplicated *after* validation, so uniqueness is only
asserted on data that is about to be, or already is, in a table."""

from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache

import numpy as np
import pandas as pd
import pandera.pandas as pa

from nimbus.common.config import VariableSpec, load_variables

# Floating-point slack for "derived column equals its definition" checks.
_EPS = 1e-6


@dataclass(frozen=True)
class TableChecks:
    table: str
    blocking: pa.DataFrameSchema
    warning: pa.DataFrameSchema


def _tz_aware(series: pd.Series) -> bool:
    return isinstance(series.dtype, pd.DatetimeTZDtype)


def _is_integer(series: pd.Series) -> bool:
    return bool(pd.api.types.is_integer_dtype(series.dtype))


def _finite_or_missing(series: pd.Series) -> pd.Series:
    """Missing (NaN) is allowed - it means "not reported" - but +/-inf never is."""
    return ~series.astype("float64").isin([np.inf, -np.inf])


def _known_variable(variables: Sequence[VariableSpec]) -> pa.Check:
    names = {v.name for v in variables}
    return pa.Check(
        lambda df: df["variable"].astype(str).isin(names),
        name="known_variable",
    )


def _range_check(
    variables: Sequence[VariableSpec], *, hard: bool, value_column: str, name: str
) -> pa.Check:
    """Row-level: `value_column` lies within its variable's range. A missing value
    passes (it is missing, not out of range); an unknown variable passes here and
    fails `known_variable` instead."""
    lows = {v.name: (v.hard_min if hard else v.plausible_min) for v in variables}
    highs = {v.name: (v.hard_max if hard else v.plausible_max) for v in variables}

    def within_variable_bounds(df: pd.DataFrame) -> pd.Series:
        variable = df["variable"].astype(str)
        low = variable.map(lows).astype("float64")
        high = variable.map(highs).astype("float64")
        value = df[value_column].astype("float64")
        inside = (value >= low) & (value <= high)
        return inside | value.isna() | low.isna()

    return pa.Check(within_variable_bounds, name=name)


def _key_columns(names: Sequence[str]) -> dict[str, pa.Column]:
    return {name: pa.Column(nullable=False) for name in names}


def _time_columns(names: Sequence[str]) -> dict[str, pa.Column]:
    return {
        name: pa.Column(nullable=False, checks=pa.Check(_tz_aware, name="timezone_aware"))
        for name in names
    }


def _unique(key: Sequence[str], enabled: bool) -> list[str] | None:
    return list(key) if enabled else None


def forecast_checks(variables: Sequence[VariableSpec], *, unique: bool = False) -> TableChecks:
    key = ["model", "location_id", "init_time", "valid_time", "variable"]
    blocking = pa.DataFrameSchema(
        {
            **_key_columns(["model", "location_id", "variable", "ingestion_mode"]),
            **_time_columns(["init_time", "valid_time"]),
            "value": pa.Column(
                "float64", nullable=True, checks=pa.Check(_finite_or_missing, name="finite")
            ),
            "lead_hours": pa.Column(nullable=False, checks=pa.Check(_is_integer, name="integer")),
        },
        checks=[
            _known_variable(variables),
            _range_check(variables, hard=True, value_column="value", name="hard_range"),
        ],
        unique=_unique(key, unique),
        report_duplicates="all",
    )
    # A valid_time before init_time is odd but harmless (the API's hourly array can
    # start before the run, and gold only scores lead >= min_lead_hours), so it is
    # flagged, not quarantined.
    warning = pa.DataFrameSchema(
        {"lead_hours": pa.Column(checks=pa.Check(lambda s: s >= 0, name="lead_not_negative"))},
        checks=[_range_check(variables, hard=False, value_column="value", name="plausible_range")],
    )
    return TableChecks("silver.forecast", blocking, warning)


def observation_checks(variables: Sequence[VariableSpec], *, unique: bool = False) -> TableChecks:
    key = ["station", "observed_at", "variable"]
    blocking = pa.DataFrameSchema(
        {
            **_key_columns(["station", "variable", "ingestion_mode", "raw_text"]),
            **_time_columns(["observed_at"]),
            "value": pa.Column(
                "float64", nullable=True, checks=pa.Check(_finite_or_missing, name="finite")
            ),
            "is_corrected": pa.Column("bool", nullable=False),
        },
        checks=[
            _known_variable(variables),
            _range_check(variables, hard=True, value_column="value", name="hard_range"),
        ],
        unique=_unique(key, unique),
        report_duplicates="all",
    )
    warning = pa.DataFrameSchema(
        {},
        checks=[_range_check(variables, hard=False, value_column="value", name="plausible_range")],
    )
    return TableChecks("silver.observation", blocking, warning)


def verification_checks(
    variables: Sequence[VariableSpec], *, tolerance_minutes: int, unique: bool = True
) -> TableChecks:
    key = ["model", "location_id", "init_time", "valid_time", "variable"]
    max_offset = tolerance_minutes * 60

    def error_matches_definition(df: pd.DataFrame) -> pd.Series:
        expected = df["forecast_value"] - df["observed_value"]
        return (df["error"] - expected).abs() <= _EPS * np.maximum(1.0, expected.abs())

    def lead_day_matches_lead_hours(df: pd.DataFrame) -> pd.Series:
        return df["lead_day"].astype("int64") == (df["lead_hours"].astype("int64") + 23) // 24

    finite = pa.Check(np.isfinite, name="finite")
    blocking = pa.DataFrameSchema(
        {
            **_key_columns(["model", "location_id", "variable", "ingestion_mode"]),
            **_time_columns(["init_time", "valid_time", "observed_at"]),
            "lead_hours": pa.Column(nullable=False, checks=pa.Check(_is_integer, name="integer")),
            "lead_day": pa.Column(
                nullable=False, checks=pa.Check(lambda s: s >= 1, name="lead_day_at_least_1")
            ),
            "forecast_value": pa.Column("float64", nullable=False, checks=finite),
            "observed_value": pa.Column("float64", nullable=False, checks=finite),
            "error": pa.Column("float64", nullable=False, checks=finite),
            "obs_offset_seconds": pa.Column(
                nullable=False,
                checks=pa.Check(lambda s: s.abs() <= max_offset, name="within_match_tolerance"),
            ),
        },
        checks=[
            _known_variable(variables),
            pa.Check(error_matches_definition, name="error_is_forecast_minus_observed"),
            pa.Check(lead_day_matches_lead_hours, name="lead_day_is_ceil_of_lead_hours"),
            _range_check(variables, hard=True, value_column="forecast_value", name="hard_range_f"),
            _range_check(variables, hard=True, value_column="observed_value", name="hard_range_o"),
        ],
        unique=_unique(key, unique),
        report_duplicates="all",
    )
    warning = pa.DataFrameSchema(
        {},
        checks=[
            _range_check(
                variables, hard=False, value_column="forecast_value", name="plausible_range_f"
            ),
            _range_check(
                variables, hard=False, value_column="observed_value", name="plausible_range_o"
            ),
        ],
    )
    return TableChecks("gold.forecast_verification", blocking, warning)


def accuracy_checks(*, unique: bool = True) -> TableChecks:
    key = ["valid_date", "location_id", "model", "variable", "lead_day"]
    finite = pa.Check(np.isfinite, name="finite")

    def statistics_are_consistent(df: pd.DataFrame) -> pd.Series:
        # RMSE >= MAE >= |bias| always holds for the same set of errors.
        return (df["rmse"] + _EPS >= df["mae"]) & (df["mae"] + _EPS >= df["bias"].abs())

    blocking = pa.DataFrameSchema(
        {
            **_key_columns(["valid_date", "location_id", "model", "variable"]),
            "lead_day": pa.Column(nullable=False, checks=pa.Check(lambda s: s >= 1, name="ge_1")),
            "n": pa.Column(nullable=False, checks=pa.Check(lambda s: s >= 1, name="n_at_least_1")),
            "bias": pa.Column("float64", nullable=False, checks=finite),
            "mae": pa.Column(
                "float64",
                nullable=False,
                checks=[finite, pa.Check(lambda s: s >= 0, name="non_negative")],
            ),
            "rmse": pa.Column(
                "float64",
                nullable=False,
                checks=[finite, pa.Check(lambda s: s >= 0, name="non_negative")],
            ),
        },
        checks=[pa.Check(statistics_are_consistent, name="rmse_ge_mae_ge_abs_bias")],
        unique=_unique(key, unique),
        report_duplicates="all",
    )
    return TableChecks("gold.accuracy_daily", blocking, pa.DataFrameSchema({}))


@lru_cache(maxsize=1)
def silver_forecast_gate() -> TableChecks:
    """The checks every silver forecast load (consumer, replay, reconcile) applies."""
    return forecast_checks(load_variables())


@lru_cache(maxsize=1)
def silver_observation_gate() -> TableChecks:
    return observation_checks(load_variables())
