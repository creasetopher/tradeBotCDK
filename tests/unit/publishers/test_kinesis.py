from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock

import boto3
import pytest

from market_data_worker.publishers.kinesis import KinesisMarketEventPublisher
from tradebot.providers.yfinance import quote_event_from_yfinance_message


def test_publish_puts_serialized_event_on_configured_stream() -> None:
    test_kinesis_client = boto3.client(
        "kinesis",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    kinesis_client = Mock(spec=test_kinesis_client)
    publisher = KinesisMarketEventPublisher(
        stream_name="tradebot-test-market-events",
        client=kinesis_client,
    )

    yfinance_message: dict[str, Any] = {
        "id": "AAPL",
        "price": 237.50,
        "time": 1788898500000,
        "dayVolume": 42_000_000,
        "exchange": "NMS",
        "marketHours": "REGULAR_MARKET",
        "quoteType": "EQUITY",
    }
    event = quote_event_from_yfinance_message(yfinance_message)

    publisher.publish(event)

    kinesis_client.put_record.assert_called_once()
    request = kinesis_client.put_record.call_args.kwargs
    assert request["StreamName"] == "tradebot-test-market-events"
    assert request["PartitionKey"] == "AAPL"

    payload = json.loads(request["Data"].decode("utf-8"))
    assert payload["event_type"] == "market.quote.v1"
    assert payload["symbol"] == "AAPL"
    assert payload["quote"]["price"] == "237.5"
    assert payload["quote"]["day_volume"] == "42000000"


def test_empty_stream_name_is_rejected() -> None:
    with pytest.raises(ValueError, match="stream_name is required"):
        KinesisMarketEventPublisher(stream_name="", client=Mock())
