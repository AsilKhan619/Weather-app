.PHONY: up down logs demo backfill drain reconcile gold test test-integration lint typecheck eval trace replay sync migrate init-topics produce-forecasts produce-observations

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

# Incremental and idempotent: recomputes only the days that saw new or revised data.
# `make gold ARGS=--full` recomputes every day.
gold:
	uv run python -m nimbus.jobs.build_gold $(ARGS)

DEMO_DAYS ?= 30

# Produce history, then ALWAYS land whatever was produced (drain + reconcile + gold),
# and only then report the producer's status. A partially failed backfill (say a
# rate limit) must not strand the events it did produce in Kafka.
define LOAD_HISTORY
@status=0; \
uv run python -m nimbus.jobs.backfill $(1) || status=$$?; \
$(MAKE) drain && $(MAKE) reconcile && $(MAKE) gold; rc=$$?; \
if [ $$status -ne 0 ]; then echo "backfill reported failures (exit $$status); landed what it produced"; exit $$status; fi; \
exit $$rc
endef

demo:
	$(call LOAD_HISTORY,--days $(DEMO_DAYS))
	@echo "Demo data loaded into bronze, silver and gold. Query it with:"
	@echo "  docker exec nimbus-postgres psql -U nimbus -d nimbus -c 'select count(*) from silver.forecast'"

backfill:
	$(call LOAD_HISTORY,--full)

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
