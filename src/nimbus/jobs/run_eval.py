"""`make eval`: run the agent eval (evals/agent_questions.yaml) and save the results.

Every question runs as a stored agent session (purpose 'eval') with the deterministic scripted
client - never a paid model: the project runs at $0 (ADR 0009 explains what the baseline score
does and does not measure). Needs a loaded database (`make demo`). Exits non-zero when the
accuracy is under `--min-accuracy` or nothing could be graded."""

import argparse
import sys

from nimbus.agent.evals import RESULTS_DIR, EvalRun, load_questions, run_eval, save_run
from nimbus.common.config import load_llm_config
from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings


def format_run(run: EvalRun) -> str:
    lines = []
    for r in run.results:
        mark = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP"}[r.status]
        lines.append(f"{mark}  {r.question_id}  ({r.tool_calls} tool calls, {r.latency_ms} ms)")
        lines.extend(f"        - {failure}" for failure in r.failures)
    s = run.summary()
    accuracy = "n/a" if s["accuracy"] is None else f"{s['accuracy']:.0%}"
    lines += [
        "",
        f"accuracy {accuracy}: {s['passed']}/{s['graded']} graded questions passed "
        f"({s['skipped']} skipped: no data to grade yet)",
        f"{s['tool_calls']} tool calls, {s['input_tokens'] + s['output_tokens']} tokens "
        f"(scripted - not billed), ${s['cost_usd']:.4f}, {s['latency_ms_total']} ms",
        f"model {s['model']}, prompt {s['prompt_version']}",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Ask Nimbus eval.")
    parser.add_argument("--only", nargs="*", help="question ids (default: all)")
    parser.add_argument("--min-accuracy", type=float, default=0.8)
    parser.add_argument("--no-save", action="store_true", help="do not write evals/results/")
    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    questions = load_questions()
    if args.only:
        questions = [q for q in questions if q.id in set(args.only)]
    run = run_eval(
        questions,
        readonly_engine=make_engine(settings, readonly=True),
        engine=make_engine(settings),
        settings=settings,
        config=load_llm_config(),
    )
    print(format_run(run))
    if not args.no_save:
        print(f"saved {save_run(run).relative_to(RESULTS_DIR.parent.parent)}")
    if run.accuracy is None:
        print("nothing could be graded - load data first (make demo)")
        sys.exit(1)
    if run.accuracy < args.min_accuracy:
        sys.exit(1)


if __name__ == "__main__":
    main()
