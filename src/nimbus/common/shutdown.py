"""Graceful shutdown on SIGTERM/SIGINT (brief section 6), shared by every
long-running producer and consumer loop so each one gets the same
flush-before-exit behavior instead of reimplementing signal handling."""

import logging
import signal
from types import FrameType

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Flips `should_stop` on SIGTERM/SIGINT so a running loop can exit
    cleanly between units of work instead of being killed mid-write."""

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown signal received", extra={"signal": signum})
        self.should_stop = True
