"""The agent loop (brief section 11, ADR 0009), written directly on the Messages API's tool
use so every step is visible:

    user question -> model turn -> tool calls? -> run each tool -> tool results -> model turn
                                        no -> final answer

Hard stops per question: `max_iterations` model turns and a `token_budget` of input + output
tokens across them. Every tool call is recorded in a trace - input, a bounded excerpt of the
output, whether it failed, how long it took, and the SQL that actually ran - and the whole
session is stored in ops.agent_sessions for the dashboard.

Tool results go back as JSON inside `tool_result` blocks, structurally separate from the
instructions, wrapped as `{"result": ...}`; the system prompt says tool output is data."""

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import Engine, text

from nimbus.agent.llm import AgentLLM
from nimbus.agent.tools import TOOLS, ToolContext, ToolError
from nimbus.common.config import LLMConfig
from nimbus.common.settings import Settings
from nimbus.llm.client import LLMUnavailableError, LLMUsage, estimate_cost_usd

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
StopReason = Literal["answered", "max_iterations", "token_budget", "error", "disabled"]
_TRACE_EXCERPT = 1500


@dataclass
class TraceStep:
    tool: str
    input: dict[str, Any]
    ok: bool
    output_excerpt: str
    latency_ms: int
    sql: str | None = None


@dataclass
class AgentResult:
    session_id: str
    question: str
    answer: str | None
    stop_reason: StopReason
    model: str
    iterations: int = 0
    usage: LLMUsage = field(default_factory=LLMUsage)
    cost_usd: float = 0.0
    latency_ms: int = 0
    trace: list[TraceStep] = field(default_factory=list)
    tool_results: list[Any] = field(default_factory=list)  # parsed, for grading

    @property
    def sql(self) -> list[str]:
        return [step.sql for step in self.trace if step.sql]


@lru_cache(maxsize=4)
def load_prompt(version: str) -> str:
    return (PROMPTS_DIR / f"{version}.md").read_text(encoding="utf-8").strip()


def _add(a: LLMUsage, b: LLMUsage) -> LLMUsage:
    return LLMUsage(
        a.input_tokens + b.input_tokens,
        a.output_tokens + b.output_tokens,
        a.cache_read_tokens + b.cache_read_tokens,
        a.cache_write_tokens + b.cache_write_tokens,
    )


def execute_tool(
    ctx: ToolContext, name: str, tool_input: dict[str, Any]
) -> tuple[str, bool, TraceStep, Any]:
    """Run one tool; returns (content for the model, is_error, trace step, parsed result)."""
    started = time.perf_counter()
    tool = TOOLS.get(name)
    result: Any
    try:
        if tool is None:
            raise ToolError(f"unknown tool {name!r}")
        result = tool.handler(ctx, tool_input)
        ok = True
    except ToolError as exc:
        result, ok = {"error": str(exc)}, False
    except Exception as exc:  # a bug in a tool must not kill the session; the model sees why
        logger.exception("tool failed", extra={"tool": name})
        result, ok = {"error": f"internal error in {name}: {type(exc).__name__}"}, False
    content = json.dumps({"result": result}, default=str)
    if len(content) > ctx.config.tool_output_chars:
        content = json.dumps(
            {
                "result_truncated": content[: ctx.config.tool_output_chars - 200],
                "note": "output cut; narrow the query or aggregate in SQL",
            }
        )
    step = TraceStep(
        tool=name,
        input=tool_input,
        ok=ok,
        output_excerpt=content[:_TRACE_EXCERPT],
        latency_ms=int((time.perf_counter() - started) * 1000),
        sql=result.get("sql") if ok and isinstance(result, dict) else None,
    )
    return content, not ok, step, result


def run_agent(
    question: str,
    client: AgentLLM,
    *,
    readonly_engine: Engine,
    engine: Engine,
    settings: Settings,
    config: LLMConfig,
    purpose: str = "ask",
    persist: bool = True,
) -> AgentResult:
    agent = config.agent
    session_id = uuid.uuid4().hex
    ctx = ToolContext(readonly_engine, engine, settings, agent, session_id)
    system = load_prompt(agent.prompt_version)
    tools = [tool.definition() for tool in TOOLS.values()]
    messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
    result = AgentResult(session_id, question, None, "max_iterations", client.model)
    started = time.perf_counter()

    for iteration in range(1, agent.max_iterations + 1):
        spent = result.usage.input_tokens + result.usage.output_tokens
        if spent >= agent.token_budget:
            result.stop_reason = "token_budget"
            break
        result.iterations = iteration
        try:
            turn = client.step(system, messages, tools)
        except LLMUnavailableError as exc:
            result.stop_reason, result.answer = "error", f"The model is unavailable: {exc}"
            break
        result.usage = _add(result.usage, turn.usage)
        messages.append({"role": "assistant", "content": turn.content})
        tool_uses = [b for b in turn.content if b.get("type") == "tool_use"]

        if turn.stop_reason in ("max_tokens", "refusal"):
            result.stop_reason = "error"
            result.answer = f"The model stopped early ({turn.stop_reason})."
            break
        if turn.stop_reason != "tool_use" or not tool_uses:
            texts = [b.get("text", "") for b in turn.content if b.get("type") == "text"]
            result.answer, result.stop_reason = "\n".join(texts).strip(), "answered"
            break

        tool_results = []
        for block in tool_uses:
            content, is_error, step, parsed = execute_tool(ctx, block["name"], block["input"])
            result.trace.append(step)
            result.tool_results.append(parsed)
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block["id"],
                    "content": content,
                    "is_error": is_error,
                }
            )
        # All results of one turn go back in a single user message.
        messages.append({"role": "user", "content": tool_results})

    result.latency_ms = int((time.perf_counter() - started) * 1000)
    result.cost_usd = estimate_cost_usd(client.model, result.usage, config)
    if persist:
        save_session(engine, result, purpose, agent.prompt_version)
    return result


def save_session(engine: Engine, result: AgentResult, purpose: str, prompt_version: str) -> None:
    trace = [step.__dict__ for step in result.trace]
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.agent_sessions (session_id, purpose, question, answer, "
                "stop_reason, model, prompt_version, iterations, tool_calls, input_tokens, "
                "output_tokens, cost_usd, latency_ms, trace) VALUES (:id, :purpose, :question, "
                ":answer, :stop, :model, :prompt, :iterations, :calls, :input, :output, :cost, "
                ":latency, CAST(:trace AS jsonb))"
            ),
            {
                "id": result.session_id,
                "purpose": purpose,
                "question": result.question,
                "answer": result.answer,
                "stop": result.stop_reason,
                "model": result.model,
                "prompt": prompt_version,
                "iterations": result.iterations,
                "calls": len(result.trace),
                "input": result.usage.input_tokens,
                "output": result.usage.output_tokens,
                "cost": result.cost_usd,
                "latency": result.latency_ms,
                "trace": json.dumps(trace, default=str),
            },
        )
