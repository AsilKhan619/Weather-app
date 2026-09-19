"""Populate silver.dim_location / dim_model / dim_variable from config/ (ADR 0005).
Idempotent: safe to run on every start; only changed rows are written."""

import logging

from sqlalchemy import Engine

from nimbus.common.config import load_locations, load_models_config, load_variables
from nimbus.common.db import chunked_upsert, make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.common.tables import dim_location_table, dim_model_table, dim_variable_table

logger = logging.getLogger(__name__)


def load_dimensions(engine: Engine) -> dict[str, int]:
    locations = [
        {
            "location_id": loc.id,
            "name": loc.name,
            "climate": loc.climate,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "elevation_m": loc.elevation_m,
            "timezone": loc.timezone,
            "station": loc.station,
        }
        for loc in load_locations()
    ]
    models = [{"model_id": m.id, "name": m.name} for m in load_models_config().models]
    variables = [
        {"variable": v.name, "unit": v.unit, "description": v.description} for v in load_variables()
    ]

    for table, key, rows in (
        (dim_location_table, "location_id", locations),
        (dim_model_table, "model_id", models),
        (dim_variable_table, "variable", variables),
    ):
        update = [c.name for c in table.columns if c.name != key]
        chunked_upsert(engine, table, [key], update, rows, only_if_changed=True)
    return {"locations": len(locations), "models": len(models), "variables": len(variables)}


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    counts = load_dimensions(make_engine(settings))
    print(f"dimensions loaded: {counts}")


if __name__ == "__main__":
    main()
