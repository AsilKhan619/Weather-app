.PHONY: up down logs demo backfill drain reconcile test test-integration lint typecheck eval trace replay sync migrate init-topics produce-forecasts produce-observations

sync:
	uv sync --all-extras

# --wait blocks until every service's healthcheck passes; without it the migration
# below races Postgres startup on a fresh clone.
up:
	docker compose up -d --wait
	docker compose ps
	$(MAKE) migrate
	$(MAKE) init-topics

migrate:
	uv run alembic upgrade head

init-topics:
	uv run python -m nimbus.jobs.init_topics

produce-forecasts:
	uv run python -m nimbus.ingestion.forecast_producer

produce-observations:
	uv run python -m nimbus.ingestion.observation_producer

down:
	docker compose down

logs:
	docker compose logs -f

drain:
	uv run python -m nimbus.streaming.bronze_sink --drain
	uv run python -m nimbus.streaming.forecast_silver --drain
	uv run python -m nimbus.streaming.observation_silver --drain

reconcile:
	uv run python -m nimbus.jobs.reconcile

DEMO_DAYS ?= 30

demo:
	uv run python -m nimbus.jobs.backfill --days $(DEMO_DAYS)
	$(MAKE) drain
	$(MAKE) reconcile
	@echo "Demo data loaded into bronze and silver. Query it with:"
	@echo "  docker exec nimbus-postgres psql -U nimbus -d nimbus -c 'select count(*) from silver.forecast'"

backfill:
	uv run python -m nimbus.jobs.backfill --full
	$(MAKE) drain
	$(MAKE) reconcile

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
