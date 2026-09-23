"""`make briefings`: the daily briefing for every location (brief section 10).

With LLM_ENABLED=false (the default) the job still builds and checks every fact sheet and
logs the skip, so the data side is exercised without a key. `--dry-run` prints the fact
sheets instead of calling anything - the exact input the LLM would receive."""

import argparse
import json
import logging
from collections import Counter
from datetime import UTC, datetime

from nimbus.common.config import load_llm_config, load_locations
from nimbus.common.db import make_engine
from nimbus.common.kafka import make_producer
from nimbus.common.logging import configure_logging
from nimbus.common.settings import Settings, get_settings
from nimbus.jobs.replay import parse_utc
from nimbus.llm.briefings import generate_briefing
from nimbus.llm.client import AnthropicBriefingClient, BriefingClient
from nimbus.llm.facts import build_fact_sheet, read_fact_inputs

logger = logging.getLogger(__name__)


def make_briefing_client(settings: Settings) -> BriefingClient | None:
    """The real client when LLM_ENABLED=true, else None (briefings disabled). There is no
    silent fallback to a fake: a misconfigured key fails the job loudly."""
    if not settings.llm_enabled:
        return None
    return AnthropicBriefingClient(
        settings.nimbus_briefing_model, load_llm_config(), settings.anthropic_api_key or None
    )


def default_as_of() -> datetime:
    """The top of the current hour: repeated runs within an hour see the same fact sheet
    and so share one cached briefing."""
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily briefings.")
    parser.add_argument("--as-of", help="UTC time to brief from (default: this hour)")
    parser.add_argument("--location", action="append", help="location id (repeatable)")
    parser.add_argument("--dry-run", action="store_true", help="print fact sheets only")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    config = load_llm_config()
    engine = make_engine(settings)
    as_of = parse_utc(args.as_of) if args.as_of else default_as_of()
    locations = [loc for loc in load_locations() if not args.location or loc.id in args.location]

    if args.dry_run:
        for location in locations:
            sheet = build_fact_sheet(
                location, as_of, read_fact_inputs(engine, location.id, as_of, config), config
            )
            print(json.dumps(sheet, indent=2, sort_keys=True))
        return

    client = make_briefing_client(settings)
    producer = make_producer(settings) if client is not None else None
    statuses: Counter[str] = Counter()
    for location in locations:
        result = generate_briefing(
            engine,
            location,
            as_of,
            client=client,
            config=config,
            producer=producer,
            trigger="daily",
            trigger_ref=as_of.date().isoformat(),
            model=settings.nimbus_briefing_model,
        )
        statuses[result.status] += 1
    summary = ", ".join(f"{status}={count}" for status, count in sorted(statuses.items()))
    llm = "enabled" if client is not None else "disabled (LLM_ENABLED=false)"
    print(f"briefings as of {as_of.isoformat()} - LLM {llm}: {summary}")


if __name__ == "__main__":
    main()
