"""`make ask Q="..."`: ask the agent one question and print the answer with its tool trace.

The agent needs a language model. The project runs at $0, so with LLM_ENABLED=false (the
default) this explains that and exits without calling anything; the tools themselves can
still be exercised through `make eval`, which uses the deterministic scripted client."""

import argparse
import json
import sys

from nimbus.agent.llm import AnthropicAgentClient
from nimbus.agent.loop import AgentResult, run_agent
from nimbus.common.config import load_llm_config
from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings


def format_result(result: AgentResult) -> str:
    lines = [result.answer or f"(no answer: {result.stop_reason})", "", "Tool calls:"]
    for i, step in enumerate(result.trace, 1):
        status = "ok" if step.ok else "FAILED"
        lines.append(
            f"  {i}. {step.tool}({json.dumps(step.input)}) - {status}, {step.latency_ms} ms"
        )
        if step.sql:
            lines.append(f"     SQL: {step.sql}")
    lines.append(
        f"\n{result.iterations} turn(s), {len(result.trace)} tool call(s), "
        f"{result.usage.input_tokens + result.usage.output_tokens} tokens, "
        f"${result.cost_usd:.4f}, {result.latency_ms} ms - stop: {result.stop_reason}"
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask Nimbus a question.")
    parser.add_argument("question")
    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    if not settings.llm_enabled:
        print(
            "The agent is off: LLM_ENABLED=false (the default - this project runs at $0, and a "
            "language model is a paid API). Nothing was sent anywhere. `make eval` exercises the "
            "tools and the loop with a deterministic scripted client instead."
        )
        sys.exit(0)

    config = load_llm_config()
    client = AnthropicAgentClient(
        settings.nimbus_agent_model, config.agent, settings.anthropic_api_key or None
    )
    result = run_agent(
        args.question,
        client,
        readonly_engine=make_engine(settings, readonly=True),
        engine=make_engine(settings),
        settings=settings,
        config=config,
    )
    print(format_result(result))


if __name__ == "__main__":
    main()
