"""LLM clients behind one small interface (brief section 10, ADR 0008).

- `AnthropicBriefingClient` calls the Claude API with a JSON-schema structured output built
  from `BriefingOutput`, then validates the reply itself. `messages.parse` would validate
  too, but it raises on an invalid reply and the response - with its token usage - is lost;
  validating here means every attempt, good or bad, is logged with what it cost.
- `FakeBriefingClient` is deterministic and never touches the network. Tests, CI and
  `make eval` always use it; nothing in the pipeline falls back to it silently.

The `anthropic` package is an optional extra (`uv sync --extra llm`) and is imported only
when the real client is built, so the platform runs without it and without a key."""

import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import ValidationError

from nimbus.common.config import LLMConfig
from nimbus.llm.schemas import BriefingOutput


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass(frozen=True)
class LLMResult:
    """One attempt. `output` is None when the reply was unusable; `error` says why."""

    output: BriefingOutput | None
    usage: LLMUsage
    latency_ms: int
    stop_reason: str | None = None
    request_id: str | None = None
    error: str | None = None


class LLMUnavailableError(Exception):
    """The call itself failed (timeout, rate limit after retries, network, 5xx). The caller
    records it and moves on: the data pipeline never waits on the LLM."""


class BriefingClient(Protocol):
    model: str

    def generate(self, system: str, user: str) -> LLMResult: ...


def estimate_cost_usd(model: str, usage: LLMUsage, config: LLMConfig) -> float:
    """List-price estimate: cache writes 1.25x input, cache reads 0.1x input. Zero for a
    model without a configured price (e.g. the fake client)."""
    price = config.prices_per_million_tokens.get(model)
    if price is None:
        return 0.0
    dollars = (
        usage.input_tokens * price.input
        + usage.cache_write_tokens * price.input * 1.25
        + usage.cache_read_tokens * price.input * 0.1
        + usage.output_tokens * price.output
    ) / 1_000_000
    return round(dollars, 6)


def validate_reply(text: str) -> tuple[BriefingOutput | None, str | None]:
    try:
        return BriefingOutput.model_validate_json(text), None
    except ValidationError as exc:
        return None, f"invalid output: {exc.error_count()} error(s): {exc.errors()[0]['msg']}"


class AnthropicBriefingClient:
    def __init__(self, model: str, config: LLMConfig, api_key: str | None) -> None:
        import anthropic  # optional extra; imported only when a real client is built

        self.model = model
        self._config = config
        self._anthropic = anthropic
        self._client = anthropic.Anthropic(
            api_key=api_key, timeout=config.timeout_seconds, max_retries=config.max_retries
        )
        self._output_config: Any = {
            "format": {
                "type": "json_schema",
                "schema": anthropic.transform_schema(BriefingOutput),
            }
        }

    def generate(self, system: str, user: str) -> LLMResult:
        anthropic = self._anthropic
        started = time.perf_counter()
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self._config.max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
                output_config=self._output_config,
            )
        except (anthropic.APIConnectionError, anthropic.APIStatusError) as exc:
            # Typed SDK errors only (timeouts are APIConnectionError). Retries already
            # happened inside the SDK; anything left is the LLM being unavailable.
            raise LLMUnavailableError(f"{type(exc).__name__}: {exc}") from exc
        latency_ms = int((time.perf_counter() - started) * 1000)

        usage = LLMUsage(
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cache_read_tokens=response.usage.cache_read_input_tokens or 0,
            cache_write_tokens=response.usage.cache_creation_input_tokens or 0,
        )
        request_id = getattr(response, "_request_id", None)
        if response.stop_reason in ("max_tokens", "refusal"):
            return LLMResult(
                None,
                usage,
                latency_ms,
                response.stop_reason,
                request_id,
                f"stopped early: {response.stop_reason}",
            )
        text = "".join(block.text for block in response.content if block.type == "text")
        output, error = validate_reply(text)
        return LLMResult(output, usage, latency_ms, response.stop_reason, request_id, error)


@dataclass
class FakeBriefingClient:
    """Deterministic stand-in: writes a briefing from the fact sheet's own numbers, so it is
    grounded by construction. Knobs let tests exercise every failure path:
    `invent_number` adds a number that is not in the sheet; `invalid_attempts` returns that
    many unusable replies first; `unavailable` raises like a timed-out API."""

    model: str = "fake-briefing-model"
    invent_number: bool = False
    invalid_attempts: int = 0
    unavailable: bool = False
    calls: list[str] = field(default_factory=list)

    def generate(self, system: str, user: str) -> LLMResult:
        import json

        self.calls.append(user)
        if self.unavailable:
            raise LLMUnavailableError("APITimeoutError: fake timeout")
        usage = LLMUsage(input_tokens=len(system + user) // 4, output_tokens=120)
        if self.invalid_attempts > 0:
            self.invalid_attempts -= 1
            output, error = validate_reply('{"headline": ""}')
            return LLMResult(output, usage, 1, "end_turn", None, error)

        sheet = json.loads(user.split("FACT SHEET:\n", 1)[1])
        return LLMResult(fake_briefing(sheet, self.invent_number), usage, 1, "end_turn")


def fake_briefing(sheet: dict[str, Any], invent_number: bool = False) -> BriefingOutput:
    name = sheet["location"]["name"]
    level = sheet["confidence"]["level"]
    best = sheet["accuracy"]["most_accurate_model"] or (sheet["models"] or ["none"])[0]
    temperature = sheet["forecast_by_model"].get(best, {}).get("temperature_2m")
    outlook = (
        f"{best} expects {temperature['min']} to {temperature['max']} degC over the next "
        f"{sheet['horizon_hours']} hours."
        if temperature
        else "No temperature forecast is available."
    )
    spread = sheet["model_spread"].get("temperature_2m")
    agreement = f" The models differ by {spread['mean']} degC on average." if spread else ""
    mae = sheet["accuracy"]["by_model"].get(best, {}).get("temperature_mae_degc")
    reason = (
        f"Lowest recent day-1 temperature error: {mae} degC."
        if mae is not None
        else "No recent accuracy data; chosen from the available models."
    )
    summary = outlook + agreement
    if invent_number:
        summary += " Gusts may reach 97 km/h."  # 97 appears nowhere in any fact sheet
    risks = [f"{a['rule']} on {a['variable']}" for a in sheet["active_alerts"]][:5]
    return BriefingOutput(
        headline=f"{name}: {level} confidence for the next {sheet['horizon_hours']} hours",
        summary=summary,
        confidence=level,
        most_reliable_model=best,
        reliable_reason=reason,
        notable_risks=risks,
    )
