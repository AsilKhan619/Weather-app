.PHONY: up down logs demo backfill drain reconcile gold quality partitions alerts briefings briefing-consumer dashboard test test-integration lint typecheck eval trace replay sync migrate init-topics produce-forecasts produce-observations

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
	uv run python -m nimbus.ingestion.forecast_producer $(ARGS)

produce-observations:
	uv run python -m nimbus.ingestion.observation_producer $(ARGS)

# Anomaly detector: live events -> weather.alert.v1 (Ctrl+C to stop; ARGS=--drain to catch up and exit)
alerts:
	uv run python -m nimbus.alerts.detector $(ARGS)

# LLM briefings (off unless LLM_ENABLED=true; needs `uv sync --extra llm`). ARGS=--dry-run prints
# the fact sheets; ARGS="--as-of 2026-09-01T12:00" briefs from a point inside loaded history.
briefings:
	uv run python -m nimbus.jobs.generate_briefings $(ARGS)

# Briefings when alerts arrive: weather.alert.v1 -> weather.briefing.v1 (ARGS=--drain to catch up and exit)
briefing-consumer:
	uv run python -m nimbus.llm.alert_briefings $(ARGS)

# Streamlit dashboard on http://localhost:8501 (needs `uv sync --extra dashboard`)
dashboard:
	uv run streamlit run dashboard/app.py

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

# Pandera checks over recently changed silver/gold rows, plus freshness; results land in
# ops.quality_results. Non-zero exit if a blocking check failed. ARGS=--all checks everything.
quality:
	uv run python -m nimbus.jobs.run_quality $(ARGS)

# Create upcoming monthly silver.forecast partitions; drop months past the retention
# window (config/storage.yaml; off by default). `make partitions ARGS=--dry-run` previews.
partitions:
	uv run python -m nimbus.jobs.manage_partitions $(ARGS)

DEMO_DAYS ?= 30

# Produce history, then ALWAYS land whatever was produced (drain + reconcile + gold + quality),
# and only then report the producer's status. A partially failed backfill (say a
# rate limit) must not strand the events it did produce in Kafka.
define LOAD_HISTORY
@status=0; \
uv run python -m nimbus.jobs.backfill $(1) || status=$$?; \
$(MAKE) drain && $(MAKE) reconcile && $(MAKE) gold && $(MAKE) quality; rc=$$?; \
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

# `make trace EVENT_ID=<id>`, or `make trace SAMPLE=observation` to pick a recent event.
trace:
	uv run python -m nimbus.jobs.trace $(if $(EVENT_ID),--event-id $(EVENT_ID),--sample $(or $(SAMPLE),forecast))

replay:
	uv run python -m nimbus.jobs.replay $(ARGS)
