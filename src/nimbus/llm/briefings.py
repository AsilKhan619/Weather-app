"""Generate, check, store and publish one briefing (brief section 10, ADR 0008).

    fact sheet -> cache lookup -> LLM (one retry on invalid output) -> grounding check
      -> gold.briefing (published or flagged) -> weather.briefing.v1 (published only)

- Identical inputs never pay twice: the briefing id is a hash of (fact sheet, prompt
  version, model), and a stored briefing with that id is reused - logged as a cache hit.
- The LLM being disabled or unavailable is an outcome, not a crash: it is logged to
  ops.llm_calls and the caller carries on.
- Delivery follows the detector's pattern: store first, publish, and mark `published_at`
  only once Kafka confirms, so a crash in between republishes rather than losing it."""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import Producer
from sqlalchemy import Engine, text

from nimbus.common.config import LLMConfig, Location
from nimbus.common.events import EventEnvelope
from nimbus.common.kafka import produce_json
from nimbus.llm.client import (
    BriefingClient,
    LLMResult,
    LLMUnavailableError,
    LLMUsage,
    estimate_cost_usd,
)
from nimbus.llm.facts import (
    build_fact_sheet,
    canonical_json,
    fact_sheet_hash,
    has_forecasts,
    read_fact_inputs,
)
from nimbus.llm.grounding import check_grounding
from nimbus.llm.schemas import BriefingOutput, BriefingPayload, Trigger

logger = logging.getLogger(__name__)

BRIEFING_TOPIC = "weather.briefing.v1"
BRIEFING_EVENT_TYPE = "briefing.generated"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
MAX_ATTEMPTS = 2  # the first try plus exactly one retry on invalid output

Outcome = Literal["success", "invalid_output", "grounding_failed", "error", "cache_hit", "disabled"]
Status = Literal[
    "published", "stored", "flagged", "cached", "no_data", "invalid", "unavailable", "disabled"
]


@dataclass(frozen=True)
class BriefingResult:
    location_id: str
    status: Status
    briefing_id: str | None = None
    failures: tuple[str, ...] = ()


@lru_cache(maxsize=8)
def load_prompt(version: str) -> str:
    return (PROMPTS_DIR / f"{version}.md").read_text(encoding="utf-8").strip()


def briefing_id(sheet_hash: str, prompt_version: str, model: str) -> str:
    return hashlib.sha256(f"{sheet_hash}|{prompt_version}|{model}".encode()).hexdigest()


def user_message(sheet: dict[str, Any]) -> str:
    return "Write the briefing for this location.\n\nFACT SHEET:\n" + canonical_json(sheet)


def log_call(
    engine: Engine,
    *,
    model: str,
    config: LLMConfig,
    outcome: Outcome,
    sheet_hash: str | None,
    attempt: int = 0,
    result: LLMResult | None = None,
    error: str | None = None,
    purpose: str = "briefing",
) -> None:
    usage = result.usage if result else LLMUsage()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.llm_calls (purpose, model, prompt_version, fact_sheet_hash, "
                "attempt, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
                "latency_ms, cost_usd, outcome, error, request_id) VALUES (:purpose, :model, "
                ":prompt_version, :hash, :attempt, :input, :output, :cache_read, :cache_write, "
                ":latency, :cost, :outcome, :error, :request_id)"
            ),
            {
                "purpose": purpose,
                "model": model,
                "prompt_version": config.prompt_version,
                "hash": sheet_hash,
                "attempt": attempt,
                "input": usage.input_tokens,
                "output": usage.output_tokens,
                "cache_read": usage.cache_read_tokens,
                "cache_write": usage.cache_write_tokens,
                "latency": result.latency_ms if result else 0,
                "cost": estimate_cost_usd(model, usage, config),
                "outcome": outcome,
                "error": error or (result.error if result else None),
                "request_id": result.request_id if result else None,
            },
        )


def _stored(engine: Engine, bid: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = (
            conn.execute(text("SELECT * FROM gold.briefing WHERE briefing_id = :id"), {"id": bid})
            .mappings()
            .first()
        )
    return dict(row) if row else None


def _store(
    engine: Engine,
    *,
    bid: str,
    location_id: str,
    as_of: datetime,
    trigger: Trigger,
    trigger_ref: str | None,
    sheet: dict[str, Any],
    sheet_hash: str,
    model: str,
    config: LLMConfig,
    output: BriefingOutput,
    failures: list[str],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO gold.briefing (briefing_id, location_id, as_of, trigger, trigger_ref, "
                "fact_sheet_hash, fact_sheet, prompt_version, model, headline, summary, "
                "confidence, most_reliable_model, reliable_reason, notable_risks, "
                "grounding_passed, grounding_failures) VALUES (:id, :loc, :as_of, :trigger, "
                ":ref, :hash, CAST(:sheet AS jsonb), :prompt_version, :model, :headline, "
                ":summary, :confidence, :best, :reason, CAST(:risks AS jsonb), :passed, "
                "CAST(:failures AS jsonb)) ON CONFLICT (briefing_id) DO NOTHING"
            ),
            {
                "id": bid,
                "loc": location_id,
                "as_of": as_of,
                "trigger": trigger,
                "ref": trigger_ref,
                "hash": sheet_hash,
                "sheet": canonical_json(sheet),
                "prompt_version": config.prompt_version,
                "model": model,
                "headline": output.headline,
                "summary": output.summary,
                "confidence": output.confidence,
                "best": output.most_reliable_model,
                "reason": output.reliable_reason,
                "risks": json.dumps(output.notable_risks),
                "passed": not failures,
                "failures": json.dumps(failures),
            },
        )


def publish(engine: Engine, producer: Producer, row: dict[str, Any]) -> bool:
    """Publish a stored, grounded briefing that has not been published yet. Returns True if
    it was sent now. A flagged briefing is never published."""
    if not row["grounding_passed"] or row["published_at"] is not None:
        return False
    payload = BriefingPayload(
        briefing_id=row["briefing_id"],
        location_id=row["location_id"],
        as_of=row["as_of"],
        trigger=row["trigger"],
        trigger_ref=row["trigger_ref"],
        model=row["model"],
        prompt_version=row["prompt_version"],
        fact_sheet_hash=row["fact_sheet_hash"],
        headline=row["headline"],
        summary=row["summary"],
        confidence=row["confidence"],
        most_reliable_model=row["most_reliable_model"],
        reliable_reason=row["reliable_reason"],
        notable_risks=list(row["notable_risks"]),
    )
    envelope = EventEnvelope[BriefingPayload](
        event_id=row["briefing_id"],
        source="briefing_generator",
        event_type=BRIEFING_EVENT_TYPE,
        produced_at=datetime.now(UTC),
        ingestion_mode="live",
        payload=payload,
    )
    produce_json(producer, BRIEFING_TOPIC, row["location_id"], envelope.model_dump(mode="json"))
    if producer.flush(10) > 0:
        raise RuntimeError("briefing delivery not confirmed; it will be retried")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE gold.briefing SET published_at = now() WHERE briefing_id = :id"),
            {"id": row["briefing_id"]},
        )
    return True


def generate_briefing(
    engine: Engine,
    location: Location,
    as_of: datetime,
    *,
    client: BriefingClient | None,
    config: LLMConfig,
    producer: Producer | None,
    trigger: Trigger = "daily",
    trigger_ref: str | None = None,
    model: str | None = None,
) -> BriefingResult:
    """`client=None` means the LLM is disabled: the fact sheet is still built (cheap, and
    it proves the data side works), the skip is logged, and nothing is generated."""
    sheet = build_fact_sheet(
        location, as_of, read_fact_inputs(engine, location.id, as_of, config), config
    )
    if not has_forecasts(sheet):
        return BriefingResult(location.id, "no_data")
    model_name = client.model if client is not None else (model or "disabled")
    sheet_hash = fact_sheet_hash(sheet)
    bid = briefing_id(sheet_hash, config.prompt_version, model_name)

    cached = _stored(engine, bid)
    if cached is not None:
        log_call(
            engine, model=model_name, config=config, outcome="cache_hit", sheet_hash=sheet_hash
        )
        if producer is not None:
            publish(engine, producer, cached)
        return BriefingResult(location.id, "cached", bid, tuple(cached["grounding_failures"]))

    if client is None:
        log_call(engine, model=model_name, config=config, outcome="disabled", sheet_hash=sheet_hash)
        return BriefingResult(location.id, "disabled")

    system, user = load_prompt(config.prompt_version), user_message(sheet)
    output: BriefingOutput | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            result = client.generate(system, user)
        except LLMUnavailableError as exc:
            log_call(
                engine,
                model=model_name,
                config=config,
                outcome="error",
                sheet_hash=sheet_hash,
                attempt=attempt,
                error=str(exc),
            )
            logger.warning(
                "LLM unavailable; briefing skipped",
                extra={"location_id": location.id, "error": str(exc)},
            )
            return BriefingResult(location.id, "unavailable")
        if result.output is None:
            log_call(
                engine,
                model=model_name,
                config=config,
                outcome="invalid_output",
                sheet_hash=sheet_hash,
                attempt=attempt,
                result=result,
            )
            continue
        output = result.output
        failures = check_grounding(output, sheet)
        log_call(
            engine,
            model=model_name,
            config=config,
            outcome="grounding_failed" if failures else "success",
            sheet_hash=sheet_hash,
            attempt=attempt,
            result=result,
            error="; ".join(failures) or None,
        )
        break
    if output is None:
        return BriefingResult(location.id, "invalid")

    _store(
        engine,
        bid=bid,
        location_id=location.id,
        as_of=as_of,
        trigger=trigger,
        trigger_ref=trigger_ref,
        sheet=sheet,
        sheet_hash=sheet_hash,
        model=model_name,
        config=config,
        output=output,
        failures=failures,
    )
    if failures:
        logger.warning(
            "briefing failed the grounding check; stored flagged, not published",
            extra={"event_id": bid, "location_id": location.id, "failures": failures},
        )
        return BriefingResult(location.id, "flagged", bid, tuple(failures))
    row = _stored(engine, bid)
    sent = producer is not None and row is not None and publish(engine, producer, row)
    return BriefingResult(location.id, "published" if sent else "stored", bid)
