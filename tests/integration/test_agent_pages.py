"""The Ask Nimbus and Replay Proposals pages against a real Postgres (brief section 15, Phase 6
acceptance: the tool trace shows in the UI; replay proposals require human approval).

Asking is exercised with the scripted client standing in for the model - the project runs at
$0 - and with the default settings the page must not build a real client at all."""

import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, text
from streamlit.testing.v1 import AppTest

from nimbus.agent import llm as agent_llm
from nimbus.agent.llm import PlannedCall, ScriptedAgentClient
from nimbus.agent.loop import run_agent
from nimbus.agent.proposals import DecisionError, decide, replay_command
from nimbus.agent.tools import TOOLS, ToolContext
from nimbus.common.config import load_llm_config
from nimbus.common.db import make_engine
from nimbus.common.settings import Settings
from nimbus.dashboard import queries

pytestmark = pytest.mark.integration

APP = Path(__file__).resolve().parents[2] / "dashboard" / "app.py"
CONFIG = load_llm_config()
COUNT_SQL = "SELECT count(*) AS n FROM gold.accuracy_daily"


@pytest.fixture
def engines(pg_settings: Settings) -> Iterator[tuple[Engine, Engine, Settings]]:
    settings = pg_settings.model_copy(update={"kafka_bootstrap_servers": "127.0.0.1:1"})
    rw, ro = make_engine(settings), make_engine(settings, readonly=True)
    with rw.begin() as conn:
        conn.execute(text("TRUNCATE ops.replay_proposals, ops.agent_sessions"))
    yield rw, ro, settings
    rw.dispose()
    ro.dispose()


def _client(answer: str = "There are {r0.rows.0.0} daily accuracy rows.") -> ScriptedAgentClient:
    return ScriptedAgentClient([PlannedCall("run_sql", {"query": COUNT_SQL})], answer)


def _session(engines: tuple[Engine, Engine, Settings], purpose: str = "eval") -> str:
    rw, ro, settings = engines
    result = run_agent(
        "How many daily accuracy rows are there?", _client(), readonly_engine=ro, engine=rw,
        settings=settings, config=CONFIG, purpose=purpose,
    )  # fmt: skip
    return result.session_id


def _propose(engines: tuple[Engine, Engine, Settings], hours_ago: int = 3) -> int:
    rw, ro, settings = engines
    ctx = ToolContext(ro, rw, settings, CONFIG.agent, _session(engines, "ask"))
    from_time = (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()
    result = TOOLS["propose_replay"].handler(
        ctx,
        {"consumer_group": "silver-forecast", "from_time": from_time, "reason": "bad transform"},
    )
    return int(result["proposal_id"])


def _page(page: str) -> AppTest:
    at = AppTest.from_file(str(APP), default_timeout=90)
    at.run()
    at.switch_page(page)
    at.run()
    assert not at.exception, at.exception
    return at


# --- deciding on proposals ----------------------------------------------------------------------


def test_a_person_decides_a_proposal_exactly_once(engines: Any) -> None:
    rw = engines[0]
    proposal = _propose(engines)

    with pytest.raises(DecisionError, match="who is deciding"):
        decide(rw, proposal, approve=True, decided_by="  ", note="")
    with pytest.raises(DecisionError, match="cannot decide its own"):
        decide(rw, proposal, approve=True, decided_by="agent", note="")
    assert decide(rw, proposal, approve=True, decided_by="Asil", note="checked lag") == "approved"
    with pytest.raises(DecisionError, match="not pending"):
        decide(rw, proposal, approve=False, decided_by="Someone else", note="")

    [row] = queries.replay_proposals(rw, "approved").itertuples()
    assert (row.decided_by, row.decision_note) == ("Asil", "checked lag")
    assert row.question == "How many daily accuracy rows are there?"  # the filing session


def test_the_replay_command_is_in_utc() -> None:
    from_time = datetime(2026, 9, 20, 8, 30, tzinfo=UTC).astimezone(
        datetime.now().astimezone().tzinfo
    )
    assert replay_command("silver-forecast", "weather.forecast.raw.v1", from_time) == (
        'make replay ARGS="offsets --group silver-forecast --topic weather.forecast.raw.v1 '
        '--from-time 2026-09-20T08:30:00"'
    )


# --- Ask Nimbus ---------------------------------------------------------------------------------


def test_ask_page_shows_a_stored_session_with_its_tool_trace_and_sql(
    engines: Any, app_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    built = []
    monkeypatch.setattr(agent_llm, "AnthropicAgentClient", lambda *a: built.append(a))
    _session(engines)

    at = _page("views/ask.py")

    assert not built  # default settings: no real client, nothing sent anywhere
    assert any("LLM_ENABLED=false" in i.value for i in at.info)
    assert at.text_input(key="question").disabled
    assert [m.label for m in at.metric][:3] == ["Sessions (7 d)", "Answered", "Tool calls"]
    assert re.fullmatch(r"There are \d+ daily accuracy rows\.", at.success[0].value)
    assert at.expander[0].label.startswith("1. run_sql - ok")
    assert any(c.language == "sql" and "gold.accuracy_daily" in c.value for c in at.code)


def test_asking_runs_the_agent_and_selects_the_new_session(
    engines: Any, app_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _session(engines)  # an older session, so the page must switch to the new one
    monkeypatch.setattr(
        agent_llm, "client_from_settings", lambda *a: _client("Fresh answer: {r0.rows.0.0} rows.")
    )
    at = _page("views/ask.py")

    at.text_input(key="question").input("How many rows?").run()
    at.button[0].click().run()

    assert not at.exception, at.exception
    assert re.fullmatch(r"Fresh answer: \d+ rows\.", at.success[0].value)
    assert at.metric[0].value == "2"
    with engines[0].connect() as conn:
        purposes = conn.execute(text("SELECT purpose FROM ops.agent_sessions")).scalars().all()
    assert sorted(purposes) == ["ask", "eval"]


# --- Replay Proposals ---------------------------------------------------------------------------


def test_replay_page_approves_a_proposal_and_runs_nothing(engines: Any, app_env: None) -> None:
    proposal = _propose(engines)
    at = _page("views/replays.py")

    assert at.subheader[0].value == "Pending (1)"
    assert any(c.language == "bash" and "--group silver-forecast" in c.value for c in at.code)

    at.button[0].click().run()  # Approve, without a name
    assert any("who is deciding" in e.value for e in at.error)

    at.text_input(key=f"who-{proposal}").input("Asil")
    at.text_input(key=f"note-{proposal}").input("consumer stopped first")
    at.button[0].click().run()

    assert not at.exception, at.exception
    assert "approved" in at.success[0].value and "Nothing has run yet" in at.success[0].value
    assert at.subheader[0].value == "Pending (0)"
    decided = at.dataframe[0].value
    assert list(decided["status"]) == ["approved"] and list(decided["decided_by"]) == ["Asil"]


def test_replay_page_rejects_a_proposal(engines: Any, app_env: None) -> None:
    proposal = _propose(engines)
    at = _page("views/replays.py")

    at.text_input(key=f"who-{proposal}").input("Asil")
    at.button[1].click().run()  # Reject

    assert not at.exception, at.exception
    assert at.success[0].value == f"Proposal #{proposal} rejected."
    assert queries.replay_proposals(engines[0], "pending").empty
