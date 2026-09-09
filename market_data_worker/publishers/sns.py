"""SNS implementation of the market-event publisher."""

from __future__ import annotations

from typing import Any

import boto3
from tradebot.events.market import MarketQuoteEvent


class SnsMarketEventPublisher:
    """Publish market quote events to an Amazon SNS topic."""

    def __init__(self, topic_arn: str, client: Any | None = None) -> None:
        if not topic_arn:
            raise ValueError("topic_arn is required")

        self._topic_arn = topic_arn
        self._client = client or boto3.client("sns")

    def publish(self, event: MarketQuoteEvent) -> None:
        """Serialize and publish an event with its type as message metadata."""
        self._client.publish(
            TopicArn=self._topic_arn,
            Message=event.model_dump_json(exclude_none=True),
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": event.event_type,
                }
            },
        )
