"""The three anomaly rules (brief section 9), as pure functions over small frames.

Frames hold one model run's hourly forecast for one location: `variable`,
`valid_time` (UTC), `value` (SI). Nothing here touches Kafka or Postgres, so each rule
is unit-tested with hand-built frames. Missing values (NaN) are ignored, and a rule
stays silent unless it has `min_overlap_hours` of data - a short overlap is not
evidence of anything."""

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from nimbus.common.config import AlertsConfig
from nimbus.common.schemas import AlertSeverity


@dataclass(frozen=True)
class Finding:
    variable: str
    metric: float
    threshold: float
    severity: AlertSeverity
    details: dict[str, Any]


def classify(metric: float, threshold: float, critical_multiplier: float) -> AlertSeverity | None:
    """None below the threshold; 'critical' at critical_multiplier x the threshold."""
    if metric < threshold:
        return None
    return "critical" if metric >= threshold * critical_multiplier else "warning"


def _window(frame: pd.DataFrame, variable: str, init: pd.Timestamp, hours: int) -> pd.Series:
    """The variable's values for valid times in (init, init + hours], indexed by valid_time."""
    rows = frame[frame["variable"].astype(str) == variable]
    in_window = (rows["valid_time"] > init) & (
        rows["valid_time"] <= init + pd.Timedelta(hours=hours)
    )
    series = rows.loc[in_window].drop_duplicates("valid_time").set_index("valid_time")["value"]
    return series.astype("float64").dropna()


def run_change(
    new: pd.DataFrame,
    previous: pd.DataFrame,
    *,
    init: pd.Timestamp,
    previous_init: pd.Timestamp,
    config: AlertsConfig,
) -> list[Finding]:
    """Rule 1: how much did a model's next-48h forecast move between two runs?
    Metric: mean absolute difference over the hours both runs cover."""
    findings: list[Finding] = []
    for variable, threshold in config.run_change.items():
        latest = _window(new, variable, init, config.horizon_hours)
        earlier = _window(previous, variable, init, config.horizon_hours)
        both = pd.concat([latest, earlier], axis=1, keys=["new", "old"], join="inner")
        if len(both) < config.min_overlap_hours:
            continue
        diff = both["new"] - both["old"]
        metric = float(diff.abs().mean())
        severity = classify(metric, threshold, config.critical_multiplier)
        if severity:
            findings.append(
                Finding(
                    variable,
                    metric,
                    threshold,
                    severity,
                    {
                        "hours_compared": len(both),
                        "previous_init": previous_init.isoformat(),
                        "mean_signed_change": float(diff.mean()),
                        "max_abs_change": float(diff.abs().max()),
                    },
                )
            )
    return findings


def model_spread(
    runs: dict[str, pd.DataFrame], *, init: pd.Timestamp, config: AlertsConfig
) -> list[Finding]:
    """Rule 2: do the models disagree about the next 48 hours? Metric: the mean, over
    hours where at least two models have a value, of (max - min) across models."""
    if len(runs) < 2:
        return []
    findings: list[Finding] = []
    for variable, threshold in config.model_spread.items():
        wide = pd.concat(
            {
                model: _window(frame, variable, init, config.horizon_hours)
                for model, frame in runs.items()
            },
            axis=1,
        )
        wide = wide[wide.notna().sum(axis=1) >= 2]
        if len(wide) < config.min_overlap_hours:
            continue
        spread = wide.max(axis=1) - wide.min(axis=1)
        metric = float(spread.mean())
        severity = classify(metric, threshold, config.critical_multiplier)
        if severity:
            findings.append(
                Finding(
                    variable,
                    metric,
                    threshold,
                    severity,
                    {
                        "hours_compared": len(wide),
                        "models": sorted(wide.columns),
                        "max_spread": float(spread.max()),
                        "model_means": {str(m): float(v) for m, v in wide.mean().items()},
                    },
                )
            )
    return findings


def observation_miss(
    variable: str, observed: float, forecasts: dict[str, float], config: AlertsConfig
) -> Finding | None:
    """Rule 3: how far is an observation from what the models said for that hour?
    Metric: |observed - mean of the models' forecasts|."""
    threshold = config.observation_miss.get(variable)
    clean = {m: v for m, v in forecasts.items() if v is not None and not np.isnan(v)}
    if threshold is None or not clean or np.isnan(observed):
        return None
    mean = float(np.mean(list(clean.values())))
    metric = abs(observed - mean)
    severity = classify(metric, threshold, config.critical_multiplier)
    if severity is None:
        return None
    return Finding(
        variable,
        metric,
        threshold,
        severity,
        {
            "observed": observed,
            "forecast_mean": mean,
            "signed_error": mean - observed,
            "forecasts": clean,
        },
    )
