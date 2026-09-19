"""Typed loaders for the YAML files under config/ (locations, models, variables, gold)."""

from pathlib import Path

import yaml
from pydantic import BaseModel

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


class Location(BaseModel):
    id: str
    name: str
    climate: str
    latitude: float
    longitude: float
    elevation_m: float
    timezone: str
    station: str


class ModelSpec(BaseModel):
    id: str
    name: str


class ModelsConfig(BaseModel):
    models: list[ModelSpec]
    run_cadence_hours: int
    run_lookback_steps: int
    variables: list[str]
    forecast_days: int
    backfill_lead_days: list[int] = [1, 2, 3, 4, 5, 6, 7]


def load_locations(path: Path = CONFIG_DIR / "locations.yaml") -> list[Location]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [Location.model_validate(entry) for entry in raw["locations"]]


def load_models_config(path: Path = CONFIG_DIR / "models.yaml") -> ModelsConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return ModelsConfig.model_validate(raw)


class VariableSpec(BaseModel):
    name: str
    unit: str
    description: str


class GoldConfig(BaseModel):
    observation_match_tolerance_minutes: int
    min_lead_hours: int
    lookback_hours: int


def load_variables(path: Path = CONFIG_DIR / "variables.yaml") -> list[VariableSpec]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [VariableSpec.model_validate(entry) for entry in raw["variables"]]


def load_gold_config(path: Path = CONFIG_DIR / "gold.yaml") -> GoldConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return GoldConfig.model_validate(raw)
