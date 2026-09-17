"""`python -m nimbus.jobs.init_topics` - idempotently create every Kafka topic
per ADR 0001. Run after `docker compose up` (wired into `make up`)."""

from nimbus.common.kafka import ensure_topics
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    ensure_topics(settings)


if __name__ == "__main__":
    main()
