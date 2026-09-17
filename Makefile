.PHONY: up down logs demo backfill test test-integration lint typecheck eval trace replay sync

sync:
	uv sync --all-extras

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	docker compose ps

down:
	docker compose down

logs:
	docker compose logs -f

demo:
	uv run python -m nimbus.jobs.backfill --days 30
	@echo "Demo data loaded. Run 'uv run streamlit run dashboard/app.py' to view it."

backfill:
	uv run python -m nimbus.jobs.backfill --full

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration -m integration

lint:
	uv run ruff check .

typecheck:
	uv run mypy

eval:
	uv run python -m nimbus.jobs.run_eval

trace:
	uv run python -m nimbus.jobs.trace --event-id $(EVENT_ID)

replay:
	uv run python -m nimbus.jobs.replay $(ARGS)
