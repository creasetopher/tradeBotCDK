"""Common interface for publishing market events."""

from __future__ import annotations

from typing import Protocol

from tradebot.events.market import MarketQuoteEvent


class MarketEventPublisher(Protocol):
    """Transport-independent interface for publishing market quote events."""

    def publish(self, event: MarketQuoteEvent) -> None:
        """Publish one market quote event."""
        ...
