from __future__ import annotations

import logging
from dataclasses import dataclass

import pytest

from market_data_worker import worker
from market_data_worker.publishers.log import LoggingMarketEventPublisher


@dataclass
class FakeMarketQuoteEvent:
    symbol: str = "AAPL"
    event_type: str = "market.quote.v1"

    def model_dump_json(self, exclude_none: bool = True) -> str:
        return '{"event_type":"market.quote.v1","symbol":"AAPL"}'


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[FakeMarketQuoteEvent] = []

    def publish(self, event: FakeMarketQuoteEvent) -> None:
        self.events.append(event)


def test_handle_market_data_message_publishes_converted_event(monkeypatch) -> None:
    event = FakeMarketQuoteEvent()
    publisher = FakePublisher()
    monkeypatch.setattr(worker, "quote_event_from_yfinance_message", lambda message: event)

    worker.handle_market_data_message({"id": "AAPL", "price": 123.45}, publisher)

    assert publisher.events == [event]


def test_log_transport_publishes_event_without_aws(caplog) -> None:
    publisher = worker.create_market_event_publisher("log")

    assert isinstance(publisher, LoggingMarketEventPublisher)
    with caplog.at_level(logging.INFO):
        publisher.publish(FakeMarketQuoteEvent())

    assert "market.quote.v1" in caplog.text
    assert '"symbol":"AAPL"' in caplog.text


@pytest.mark.parametrize("transport", ["sqs", "firehose", "unknown"])
# tests that the factory raises a ValueError with a message indicating the unsupported transport when an unsupported transport is provided
def test_factory_rejects_unsupported_transport(transport: str) -> None:
    with pytest.raises(
        ValueError,
        match=f"Unsupported MARKET_EVENT_TRANSPORT: {transport}",
    ):
        worker.create_market_event_publisher(transport)
