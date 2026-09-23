"""The fact sheet: everything a briefing may say, computed by code (brief section 10).

The LLM sees nothing else. It gets each model's next-48-hour forecast summary, how much the
models disagree, each model's recent accuracy at the location, the active alerts, and a
confidence level that code has already decided from model agreement - the LLM explains the
confidence, it does not choose it.

Numbers are in display units (degC, hPa, m/s) and rounded to one decimal, so a briefing
can quote them exactly and the grounding check can hold it to them. The sheet is a plain
JSON-serialisable dict; its hash (canonical JSON) is the cache key, so identical inputs
never trigger a second paid call.

`build_fact_sheet` is pure and unit-tested; `read_fact_inputs` is the Postgres side."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import Engine, text

from nimbus.common.config import LLMConfig, Location
from nimbus.common.units import display, display_error, display_unit

HORIZON_HOURS = 48
_TEMPERATURE = "temperature_2m"
_DECIMALS = 1


@dataclass(frozen=True)
class FactInputs:
    """forecasts: model, variable, valid_time, value (SI) - the latest forecast issued at or
    before `as_of` for each hour of the window. accuracy: model, n, mae (K). alerts: rule,
    severity, variable, subject, metric (SI), event_time."""

    forecasts: pd.DataFrame
    accuracy: pd.DataFrame
    alerts: pd.DataFrame


def _r(value: float) -> float:
    return round(float(value), _DECIMALS)


def _model_summaries(forecasts: pd.DataFrame) -> dict[str, dict[str, dict[str, float]]]:
    """model -> variable -> min/mean/max over the window, in display units."""
    out: dict[str, dict[str, dict[str, float]]] = {}
    for (model, variable), group in forecasts.groupby(["model", "variable"], sort=True):
        values = display(str(variable), group["value"].astype("float64"))
        out.setdefault(str(model), {})[str(variable)] = {
            "min": _r(values.min()),
            "mean": _r(values.mean()),
            "max": _r(values.max()),
        }
    return out


def _spread(forecasts: pd.DataFrame) -> dict[str, dict[str, float]]:
    """variable -> mean and max over hours of (max - min across models), display units.
    Only hours where at least two models have a value count."""
    out: dict[str, dict[str, float]] = {}
    for variable, group in forecasts.groupby("variable", sort=True):
        wide = group.pivot_table(index="valid_time", columns="model", values="value")
        wide = wide[wide.notna().sum(axis=1) >= 2]
        if wide.empty:
            continue
        spread = display_error(str(variable), wide.max(axis=1) - wide.min(axis=1))
        out[str(variable)] = {"mean": _r(spread.mean()), "max": _r(spread.max())}
    return out


def confidence_level(
    temperature_spread_k: float | None, models: int, config: LLMConfig
) -> dict[str, Any]:
    """High / medium / low from how closely the models agree on temperature."""
    thresholds = config.confidence
    if models < 2 or temperature_spread_k is None:
        return {"level": "low", "basis": "fewer than two models cover the next 48 hours"}
    if temperature_spread_k <= thresholds.high_max_spread_k:
        level = "high"
    elif temperature_spread_k <= thresholds.medium_max_spread_k:
        level = "medium"
    else:
        level = "low"
    return {
        "level": level,
        "basis": "mean temperature spread between models over the next 48 hours",
        "temperature_spread_degc": _r(temperature_spread_k),
        "high_if_at_most_degc": thresholds.high_max_spread_k,
        "medium_if_at_most_degc": thresholds.medium_max_spread_k,
    }


def _accuracy(accuracy: pd.DataFrame, window_days: int) -> dict[str, Any]:
    if accuracy.empty:
        return {"window_days": window_days, "by_model": {}, "most_accurate_model": None}
    ranked = accuracy.sort_values(["mae", "model"])
    by_model = {
        str(model): {"temperature_mae_degc": _r(mae), "verified_forecasts": int(n)}
        for model, mae, n in zip(ranked["model"], ranked["mae"], ranked["n"], strict=True)
    }
    return {
        "window_days": window_days,
        "lead_days": 1,
        "by_model": by_model,
        "most_accurate_model": str(ranked.iloc[0]["model"]),
    }


def _alerts(alerts: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    # A total order: two models' run-change alerts in one cycle tie on time, rule and
    # variable, and an unstable order would change the hash - and pay for the same facts twice.
    ordered = alerts.assign(_subject=alerts["subject"].fillna("").astype(str)).sort_values(
        ["event_time", "rule", "variable", "_subject", "severity", "metric"], kind="stable"
    )
    for row in ordered.to_dict("records"):
        variable = str(row["variable"])
        subject = row["subject"]
        out.append(
            {
                "rule": str(row["rule"]),
                "severity": str(row["severity"]),
                "variable": variable,
                "subject": None if subject is None or pd.isna(subject) else str(subject),
                "size": _r(display_error(variable, float(row["metric"]))),
                "unit": display_unit(variable),
            }
        )
    return out


def build_fact_sheet(
    location: Location, as_of: datetime, inputs: FactInputs, config: LLMConfig
) -> dict[str, Any]:
    forecasts = inputs.forecasts.dropna(subset=["value"])
    models = sorted(forecasts["model"].astype(str).unique()) if not forecasts.empty else []
    spread = _spread(forecasts) if not forecasts.empty else {}
    temperature_models = (
        forecasts.loc[forecasts["variable"] == _TEMPERATURE, "model"].nunique()
        if not forecasts.empty
        else 0
    )
    temperature_spread = spread.get(_TEMPERATURE, {}).get("mean")
    return {
        "location": {"id": location.id, "name": location.name, "station": location.station},
        "as_of": as_of.isoformat(),
        "date": as_of.astimezone(UTC).date().isoformat(),
        "horizon_hours": HORIZON_HOURS,
        "units": {v: display_unit(v) for v in sorted(forecasts["variable"].unique())}
        if not forecasts.empty
        else {},
        "models": models,
        "forecast_by_model": _model_summaries(forecasts) if not forecasts.empty else {},
        "model_spread": spread,
        "confidence": confidence_level(temperature_spread, int(temperature_models), config),
        "accuracy": _accuracy(inputs.accuracy, config.accuracy_window_days),
        "active_alerts": _alerts(inputs.alerts),
    }


def canonical_json(sheet: dict[str, Any]) -> str:
    return json.dumps(sheet, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fact_sheet_hash(sheet: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(sheet).encode("utf-8")).hexdigest()


def has_forecasts(sheet: dict[str, Any]) -> bool:
    return bool(sheet["models"])


# --- Postgres -------------------------------------------------------------------------------


def read_fact_inputs(
    engine: Engine, location_id: str, as_of: datetime, config: LLMConfig
) -> FactInputs:
    """For each (model, variable, hour) in the next 48 hours, the most recent forecast issued
    at or before `as_of`. That works the same for live runs and for backfilled rows (whose
    derived init times make a 'run' sparse), and it never peeks at a forecast issued later."""
    end = as_of + timedelta(hours=HORIZON_HOURS)
    as_of_day = as_of.astimezone(UTC).date()
    with engine.connect() as conn:
        forecasts = pd.read_sql(
            text(
                "SELECT DISTINCT ON (model, variable, valid_time) model, variable, valid_time, "
                "value FROM silver.forecast WHERE location_id = :loc "
                "AND valid_time > :as_of AND valid_time <= :end AND init_time <= :as_of "
                "AND value IS NOT NULL "
                "ORDER BY model, variable, valid_time, init_time DESC"
            ),
            conn,
            params={"loc": location_id, "as_of": as_of, "end": end},
        )
        accuracy = pd.read_sql(
            text(
                "SELECT model, sum(n) AS n, sum(n * mae) / sum(n) AS mae "
                "FROM gold.accuracy_daily WHERE location_id = :loc "
                "AND variable = 'temperature_2m' AND lead_day = 1 "
                "AND valid_date >= :first_day AND valid_date < :as_of_day GROUP BY model"
            ),
            conn,
            # Whole UTC days strictly before as_of, computed here rather than by casting in
            # SQL (which would depend on the session time zone): the as-of day itself is
            # partly in the future of as_of, so it is left out.
            params={
                "loc": location_id,
                "first_day": as_of_day - timedelta(days=config.accuracy_window_days),
                "as_of_day": as_of_day,
            },
        )
        alerts = pd.read_sql(
            text(
                "SELECT rule, severity, variable, coalesce(model, station) AS subject, metric, "
                "event_time FROM gold.alert WHERE location_id = :loc "
                "AND event_time > :since AND event_time <= :as_of "
                "ORDER BY event_time, rule, variable, subject, severity, metric"
            ),
            conn,
            params={
                "loc": location_id,
                "as_of": as_of,
                "since": as_of - timedelta(hours=config.active_alert_hours),
            },
        )
    if not forecasts.empty:
        forecasts["valid_time"] = pd.to_datetime(forecasts["valid_time"], utc=True)
        forecasts["value"] = forecasts["value"].astype("float64")
    return FactInputs(forecasts, accuracy, alerts)
