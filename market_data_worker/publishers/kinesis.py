"""Kinesis implementation of the market-event publisher."""

from __future__ import annotations

from typing import Any

import boto3
from tradebot.events.market import MarketQuoteEvent


class KinesisMarketEventPublisher:
    """Publish market quote events to an Amazon Kinesis Data Stream."""

    def __init__(self, stream_name: str, client: Any | None = None) -> None:
        if not stream_name:
            raise ValueError("stream_name is required")

        self._stream_name = stream_name
        self._client = client or boto3.client("kinesis")

    def publish(self, event: MarketQuoteEvent) -> None:
        """Serialize and publish an event, partitioned by ticker symbol."""
        self._client.put_record(
            StreamName=self._stream_name,
            PartitionKey=event.symbol,
            Data=event.model_dump_json(exclude_none=True).encode("utf-8"),
        )
