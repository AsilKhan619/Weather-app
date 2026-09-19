"""`make gold`: build the gold layer incrementally (or `--full` to recompute every
day). Loads the dimensions first so verification always has the station mapping."""

import argparse

from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.gold.build import build_gold
from nimbus.jobs.load_dimensions import load_dimensions


def main() -> None:
    parser = argparse.ArgumentParser(description="Build gold.forecast_verification/accuracy_daily.")
    parser.add_argument("--full", action="store_true", help="recompute every day, ignore watermark")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)

    load_dimensions(engine)
    result = build_gold(engine, full=args.full)
    print(
        f"gold build ({result.run_kind}): {len(result.days)} day(s) recomputed, "
        f"{result.matched} verified forecast rows, watermark {result.watermark.isoformat()}"
    )


if __name__ == "__main__":
    main()
