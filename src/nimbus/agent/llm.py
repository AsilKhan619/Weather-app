"""LLM clients for the agent (brief section 11, ADR 0009): one small interface, an Anthropic
implementation, and a deterministic scripted client.

A client takes one step: given the system prompt, the conversation so far and the tool
definitions, it returns the assistant's content blocks (plain dicts in the API's shape, so the
loop can append them verbatim - including any thinking blocks the model returns), why it
stopped, and the tokens it used.

The project runs at $0, so only the scripted client is ever exercised: by the tests, by
`make eval` (as a deterministic baseline) and by the dashboard. `AnthropicAgentClient` is
tested only against stubbed SDK replies."""

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from nimbus.common.config import AgentConfig
from nimbus.llm.client import LLMUnavailableError, LLMUsage


@dataclass(frozen=True)
class AgentTurn:
    content: list[dict[str, Any]]
    stop_reason: str | None
    usage: LLMUsage
    latency_ms: int
    request_id: str | None = None


class AgentLLM(Protocol):
    model: str

    def step(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> AgentTurn: ...


class AnthropicAgentClient:
    """The raw Messages API with tool use - no agent framework, no Tool Runner - so every
    request and every tool round trip is code in this repository (the brief asks for that)."""

    def __init__(self, model: str, config: AgentConfig, api_key: str | None) -> None:
        import anthropic  # optional extra; imported only when a real client is built

        self.model = model
        self._config = config
        self._anthropic = anthropic
        self._client = anthropic.Anthropic(api_key=api_key, timeout=60.0, max_retries=2)

    def step(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> AgentTurn:
        anthropic = self._anthropic
        started = time.perf_counter()
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self._config.max_tokens,
                system=system,
                messages=messages,  # type: ignore[arg-type]  # API-shaped dicts
                tools=tools,  # type: ignore[arg-type]
            )
        except (anthropic.APIConnectionError, anthropic.APIStatusError) as exc:
            raise LLMUnavailableError(f"{type(exc).__name__}: {exc}") from exc
        return AgentTurn(
            content=[block.to_dict() for block in response.content],
            stop_reason=response.stop_reason,
            usage=LLMUsage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                cache_read_tokens=response.usage.cache_read_input_tokens or 0,
                cache_write_tokens=response.usage.cache_creation_input_tokens or 0,
            ),
            latency_ms=int((time.perf_counter() - started) * 1000),
            request_id=getattr(response, "_request_id", None),
        )


@dataclass(frozen=True)
class PlannedCall:
    tool: str
    input: dict[str, Any]


_PLACEHOLDER = re.compile(r"\{r(\d+)((?:\.[\w-]+)*)(?::([^}]*))?\}")


def _lookup(value: Any, path: str) -> Any:
    for part in [p for p in path.split(".") if p]:
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value


def render_answer(template: str, results: list[Any]) -> str:
    """Fill `{r0.rows.0.model}` / `{r1.rows.0.1:.2f}` from tool results (r0 = first call)."""

    def replace(match: re.Match[str]) -> str:
        value = _lookup(results[int(match.group(1))], match.group(2))
        spec = match.group(3)
        return format(value, spec) if spec else str(value)

    return _PLACEHOLDER.sub(replace, template)


@dataclass
class ScriptedAgentClient:
    """A deterministic agent that follows a fixed plan: it makes the planned tool calls one per
    turn, then writes the answer template filled from the tool results it received. It is not
    a language model and understands nothing; it exercises the loop, the tools, the SQL guard,
    the data and the grading exactly as a model's calls would. Knobs let tests reach the
    loop's limits (`repeat_forever`) and budget (`tokens_per_turn`)."""

    plan: list[PlannedCall]
    answer_template: str
    model: str = "scripted-agent"
    tokens_per_turn: int = 500
    repeat_forever: bool = False
    turns: int = 0
    seen_tool_results: list[Any] = field(default_factory=list)

    def step(
        self, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> AgentTurn:
        self.turns += 1
        usage = LLMUsage(input_tokens=self.tokens_per_turn, output_tokens=60)
        calls_made = sum(
            1
            for m in messages
            if m["role"] == "assistant"
            for b in m["content"]
            if b.get("type") == "tool_use"
        )
        if self.repeat_forever or calls_made < len(self.plan):
            call = self.plan[calls_made % len(self.plan)] if self.plan else None
            if call is not None:
                block = {
                    "type": "tool_use",
                    "id": f"toolu_scripted_{calls_made}",
                    "name": call.tool,
                    "input": call.input,
                }
                return AgentTurn([block], "tool_use", usage, 1)
        results = []
        for m in messages:
            if m["role"] == "user" and isinstance(m["content"], list):
                for b in m["content"]:
                    if b.get("type") == "tool_result":
                        results.append(json.loads(b["content"]).get("result"))
        self.seen_tool_results = results
        try:
            answer = render_answer(self.answer_template, results)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            answer = f"I could not find the answer in the tool results ({type(exc).__name__})."
        return AgentTurn([{"type": "text", "text": answer}], "end_turn", usage, 1)
