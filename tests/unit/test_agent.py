"""Phase 6 without a database, Kafka or network: the SQL guard (the brief's acceptance test -
non-SELECT SQL is rejected), the agent loop's stops and error handling with stub tools, the
scripted client, and the real Anthropic client against stubbed SDK replies."""

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from nimbus.agent import llm as agent_llm
from nimbus.agent import loop as agent_loop
from nimbus.agent.llm import (
    AgentTurn,
    AnthropicAgentClient,
    PlannedCall,
    ScriptedAgentClient,
    render_answer,
)
from nimbus.agent.sql_guard import UnsafeSQLError, validate_select
from nimbus.agent.tools import TOOLS, Tool, ToolError, clean_text
from nimbus.common.config import load_llm_config
from nimbus.common.settings import Settings
from nimbus.llm.client import LLMUnavailableError, LLMUsage

CONFIG = load_llm_config()

# --- the SQL guard ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT model, avg(mae) FROM gold.accuracy_daily GROUP BY model",
        "WITH a AS (SELECT * FROM gold.accuracy_daily) SELECT * FROM a",
        "SELECT 1 FROM gold.accuracy_daily UNION ALL SELECT 2 FROM silver.forecast",
        "SELECT d.climate, count(*) FROM gold.accuracy_daily a "
        "JOIN silver.dim_location d USING (location_id) GROUP BY 1",
        "SELECT date_trunc('day', valid_time), round(avg(value)::numeric, 2) "
        "FROM silver.forecast GROUP BY 1",
        "SELECT max(observed_at) FROM silver.observation -- trailing comment",
    ],
)
def test_read_only_queries_are_accepted(sql: str) -> None:
    safe = validate_select(sql)
    assert safe.tables and safe.sql


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("DELETE FROM gold.alert", "only SELECT"),
        ("UPDATE gold.alert SET severity = 'x'", "only SELECT"),
        ("INSERT INTO ops.replay_proposals SELECT * FROM ops.replay_proposals", "only SELECT"),
        ("DROP TABLE gold.alert", "only SELECT"),
        ("CREATE TABLE gold.t AS SELECT 1", "only SELECT"),
        ("TRUNCATE gold.alert", "only SELECT"),
        ("COPY gold.alert TO '/tmp/x'", "only SELECT"),
        ("SET ROLE postgres", "only SELECT"),
        ("EXPLAIN ANALYZE DELETE FROM gold.alert", "only SELECT"),
        ("SELECT 1 FROM gold.alert; DROP TABLE gold.alert", "exactly one statement"),
        ("WITH d AS (DELETE FROM gold.alert RETURNING *) SELECT * FROM d", "Delete"),
        ("SELECT * INTO gold.copy FROM gold.alert", "Into"),
        ("SELECT * FROM gold.alert FOR UPDATE", "Lock"),
        ("SELECT pg_sleep(600) FROM gold.alert", "pg_sleep"),
        ("SELECT set_config('role', 'postgres', false) FROM gold.alert", "set_config"),
        ("SELECT lo_import('/etc/passwd') FROM gold.alert", "lo_import"),
        ("SELECT query_to_xml('DELETE FROM gold.alert', true, true, '') FROM gold.alert",
         "query_to_xml"),
        ("SELECT * FROM pg_catalog.pg_authid", "not allowed"),
        ("SELECT * FROM information_schema.tables", "not allowed"),
        ("SELECT * FROM accuracy_daily", "not allowed"),
        # table-valued functions count as unqualified tables: not available to the agent
        ("SELECT count(*) FROM gold.alert, generate_series(1, 10)", "not allowed"),
        ("SELECT 1", "at least one"),
        ("", "exactly one statement"),
        ("SELEC model FRM gold.alert", "could not parse"),
    ],
)  # fmt: skip
def test_anything_but_a_read_only_select_is_rejected(sql: str, reason: str) -> None:
    """Brief section 15, Phase 6 acceptance: non-SELECT SQL is rejected - including writes
    hidden inside a SELECT, which a top-level type check would let through."""
    with pytest.raises(UnsafeSQLError, match=reason):
        validate_select(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "select model from gold.accuracy_daily /* ; drop table x */",
        # a line comment that would close a block comment if it were re-emitted as one
        "select model from gold.accuracy_daily -- */ ; drop table gold.alert; /*",
    ],
)
def test_the_query_that_runs_is_regenerated_without_comments(sql: str) -> None:
    assert validate_select(sql).sql == "SELECT model FROM gold.accuracy_daily"


# --- tool definitions and untrusted text ---------------------------------------------------------


def test_every_tool_has_a_strict_object_schema() -> None:
    assert set(TOOLS) == {
        "describe_data", "run_sql", "get_leaderboard", "get_pipeline_health", "sample_dlq",
        "propose_replay",
    }  # fmt: skip
    for tool in TOOLS.values():
        schema = tool.definition()["input_schema"]
        assert schema["type"] == "object" and schema["additionalProperties"] is False


def test_untrusted_text_is_stripped_of_control_characters_and_bounded() -> None:
    assert clean_text("a\x00b\x1bc\nd", 100) == "a b c\nd"
    assert clean_text("x" * 50, 10) == "xxxxxxx..."


def test_the_semantic_layer_names_real_tables_and_example_queries_pass_the_guard() -> None:
    from nimbus.agent.tools import semantic_layer

    layer = semantic_layer()
    assert "gold.accuracy_daily" in layer["tables"]
    for example in layer["example_queries"]:
        validate_select(example["sql"])  # the integration suite also executes them


def test_the_whole_semantic_layer_reaches_the_model_uncut() -> None:
    """describe_data once outgrew the tool-output cap by 7 characters, which would have handed
    the model a truncated string instead of the map of the data."""
    content, is_error, _, _ = agent_loop.execute_tool(
        MagicMock(config=CONFIG.agent), "describe_data", {}
    )
    assert not is_error
    assert "result_truncated" not in json.loads(content)
    assert len(content) < CONFIG.agent.tool_output_chars * 0.75  # room to grow


# --- the loop, with stub tools ------------------------------------------------------------------


def _stub_tool(name: str, handler: Any) -> Tool:
    return Tool(name, "stub", {"type": "object", "properties": {}}, handler)


@pytest.fixture
def stub_tools(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    calls: dict[str, Any] = {"count": 0}

    def answer(ctx: Any, args: dict[str, Any]) -> dict[str, Any]:
        calls["count"] += 1
        return {"rows": [["icon_seamless", 1.27]], "sql": "SELECT 1 FROM gold.accuracy_daily"}

    def broken(ctx: Any, args: dict[str, Any]) -> dict[str, Any]:
        raise ToolError("no such location")

    def crashing(ctx: Any, args: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("bug")

    monkeypatch.setattr(
        agent_loop,
        "TOOLS",
        {
            "lookup": _stub_tool("lookup", answer),
            "broken": _stub_tool("broken", broken),
            "crashing": _stub_tool("crashing", crashing),
        },
    )
    return calls


def _run(client: Any) -> agent_loop.AgentResult:
    return agent_loop.run_agent(
        "Which model is best?", client, readonly_engine=MagicMock(), engine=MagicMock(),
        settings=Settings(_env_file=None), config=CONFIG, persist=False,
    )  # fmt: skip


def test_a_planned_tool_call_is_run_and_its_result_fills_the_answer(
    stub_tools: dict[str, Any],
) -> None:
    client = ScriptedAgentClient(
        [PlannedCall("lookup", {})], "{r0.rows.0.0} leads at {r0.rows.0.1:.2f} K"
    )
    result = _run(client)

    assert result.stop_reason == "answered"
    assert result.answer == "icon_seamless leads at 1.27 K"
    assert (result.iterations, len(result.trace), stub_tools["count"]) == (2, 1, 1)
    assert result.sql == ["SELECT 1 FROM gold.accuracy_daily"]
    assert result.usage.input_tokens == 1000  # two turns x 500


def test_the_loop_stops_at_max_iterations(stub_tools: dict[str, Any]) -> None:
    result = _run(ScriptedAgentClient([PlannedCall("lookup", {})], "never", repeat_forever=True))
    assert result.stop_reason == "max_iterations"
    assert result.iterations == CONFIG.agent.max_iterations
    assert result.answer is None


def test_the_loop_stops_at_the_token_budget(stub_tools: dict[str, Any]) -> None:
    per_turn = CONFIG.agent.token_budget // 3
    result = _run(
        ScriptedAgentClient(
            [PlannedCall("lookup", {})], "never", repeat_forever=True, tokens_per_turn=per_turn
        )
    )
    assert result.stop_reason == "token_budget"
    assert result.iterations == 3  # the fourth turn would start over budget


def test_tool_failures_go_back_to_the_model_as_errors(stub_tools: dict[str, Any]) -> None:
    client = ScriptedAgentClient(
        [PlannedCall("broken", {}), PlannedCall("crashing", {}), PlannedCall("missing", {})],
        "done",
    )
    result = _run(client)

    assert result.stop_reason == "answered"
    assert [(s.tool, s.ok) for s in result.trace] == [
        ("broken", False), ("crashing", False), ("missing", False),
    ]  # fmt: skip
    assert "no such location" in result.trace[0].output_excerpt
    assert "internal error in crashing: RuntimeError" in result.trace[1].output_excerpt
    assert "unknown tool" in result.trace[2].output_excerpt


def test_an_unavailable_model_ends_the_session_cleanly(stub_tools: dict[str, Any]) -> None:
    client = MagicMock()
    client.model = "x"
    client.step.side_effect = LLMUnavailableError("APITimeoutError: slow")
    result = _run(client)
    assert result.stop_reason == "error" and "unavailable" in (result.answer or "")


def test_a_truncated_turn_is_an_error_not_a_half_run_tool_call(
    stub_tools: dict[str, Any],
) -> None:
    client = MagicMock()
    client.model = "x"
    client.step.return_value = AgentTurn(
        [{"type": "tool_use", "id": "t1", "name": "lookup", "input": {}}],
        "max_tokens",
        LLMUsage(10, 10),
        1,
    )
    result = _run(client)
    assert result.stop_reason == "error" and stub_tools["count"] == 0


def test_oversized_tool_output_is_cut_before_it_reaches_the_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agent_loop,
        "TOOLS",
        {"big": _stub_tool("big", lambda ctx, args: {"rows": ["x" * 50_000]})},
    )
    client = ScriptedAgentClient([PlannedCall("big", {})], "ok")
    _run(client)
    sent = client.seen_tool_results
    assert sent == [None]  # the model got the truncated form, not a 50 kB result


def test_render_answer() -> None:
    results = [{"rows": [{"model": "a", "mae": 1.234}]}, {"rows": [[3, "x"]]}]
    assert render_answer("{r0.rows.0.model} {r0.rows.0.mae:.1f} {r1.rows.0.1}", results) == (
        "a 1.2 x"
    )


# --- the real client, against stubbed replies -------------------------------------------------


def _message(content: list[dict[str, Any]], stop: str) -> Any:
    import anthropic

    return anthropic.types.Message.model_validate(
        {
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-sonnet-5",
            "content": content, "stop_reason": stop, "stop_sequence": None,
            "usage": {"input_tokens": 1200, "output_tokens": 80},
        }
    )  # fmt: skip


def test_real_client_sends_tools_and_returns_replayable_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AnthropicAgentClient("claude-sonnet-5", CONFIG.agent, api_key="test-not-real")
    sent: list[dict[str, Any]] = []
    reply = _message(
        [
            {"type": "text", "text": "Checking."},
            {"type": "tool_use", "id": "toolu_1", "name": "describe_data", "input": {}},
        ],
        "tool_use",
    )

    def create(**kw: Any) -> Any:
        sent.append(kw)
        return reply

    monkeypatch.setattr(client._client.messages, "create", create)
    tools = [t.definition() for t in TOOLS.values()]

    turn = client.step("system", [{"role": "user", "content": "q"}], tools)

    assert turn.stop_reason == "tool_use"
    assert turn.content[1] == {
        "type": "tool_use", "id": "toolu_1", "name": "describe_data", "input": {},
    }  # fmt: skip
    json.dumps(turn.content)  # plain data, safe to append and store
    assert turn.usage == LLMUsage(input_tokens=1200, output_tokens=80)
    assert sent[0]["tools"] == tools and sent[0]["model"] == "claude-sonnet-5"


def test_real_client_maps_api_failures_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    import anthropic
    import httpx2

    client = AnthropicAgentClient("claude-sonnet-5", CONFIG.agent, api_key="test-not-real")
    error = anthropic.APIConnectionError(request=httpx2.Request("POST", "https://x"))

    def fail(**kw: Any) -> Any:
        raise error

    monkeypatch.setattr(client._client.messages, "create", fail)
    with pytest.raises(LLMUnavailableError, match="APIConnectionError"):
        client.step("s", [], [])


def test_the_agent_cli_refuses_to_call_a_paid_model_by_default(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """$0 guarantee: with default settings `make ask` builds no client and sends nothing."""
    import sys

    from nimbus.common import settings as settings_module
    from nimbus.jobs import ask

    monkeypatch.delenv("LLM_ENABLED", raising=False)
    monkeypatch.setattr(sys, "argv", ["ask", "Which model is best?"])
    monkeypatch.setattr(ask, "get_settings", lambda: settings_module.Settings(_env_file=None))
    built = MagicMock(side_effect=AssertionError("a paid client was built"))
    monkeypatch.setattr(agent_llm, "AnthropicAgentClient", built)

    with pytest.raises(SystemExit) as exit_info:
        ask.main()
    assert exit_info.value.code == 0
    assert "LLM_ENABLED=false" in capsys.readouterr().out


def test_no_real_client_exists_unless_the_llm_is_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    built = MagicMock(side_effect=AssertionError("a paid client was built"))
    monkeypatch.setattr(agent_llm, "AnthropicAgentClient", built)
    monkeypatch.delenv("LLM_ENABLED", raising=False)

    assert agent_llm.client_from_settings(Settings(_env_file=None), CONFIG.agent) is None
