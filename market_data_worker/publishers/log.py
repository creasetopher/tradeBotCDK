"""Local logging implementation of the market-event publisher."""

from __future__ import annotations

import logging

from tradebot.events.market import MarketQuoteEvent


class LoggingMarketEventPublisher:
    """Write serialized market events to a logger instead of an AWS service."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def publish(self, event: MarketQuoteEvent) -> None:
        """Log one event as compact JSON."""
        self._logger.info(
            "Published market event: %s",
            event.model_dump_json(exclude_none=True),
        )
