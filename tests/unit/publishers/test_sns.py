from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock

import boto3
import pytest

from market_data_worker.publishers.sns import SnsMarketEventPublisher
from tradebot.providers.yfinance import quote_event_from_yfinance_message


def test_publish_sends_serialized_event_to_configured_topic() -> None:
    test_sns_client = boto3.client(
        "sns",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sns_client = Mock(spec=test_sns_client)
    topic_arn = "arn:aws:sns:us-east-1:123456789012:tradebot-test-market-events"
    publisher = SnsMarketEventPublisher(topic_arn=topic_arn, client=sns_client)

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

    sns_client.publish.assert_called_once()
    request = sns_client.publish.call_args.kwargs
    assert request["TopicArn"] == topic_arn
    assert request["MessageAttributes"] == {
        "event_type": {
            "DataType": "String",
            "StringValue": "market.quote.v1",
        }
    }

    payload = json.loads(request["Message"])
    assert payload["event_type"] == "market.quote.v1"
    assert payload["symbol"] == "AAPL"
    assert payload["quote"]["price"] == "237.5"
    assert payload["quote"]["day_volume"] == "42000000"


def test_empty_topic_arn_is_rejected() -> None:
    with pytest.raises(ValueError, match="topic_arn is required"):
        SnsMarketEventPublisher(topic_arn="", client=Mock())


def test_unexpected_yfinance_message_is_not_published() -> None:
    test_sns_client = boto3.client(
        "sns",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sns_client = Mock(spec=test_sns_client)
    publisher = SnsMarketEventPublisher(
        topic_arn="arn:aws:sns:us-east-1:123456789012:tradebot-test-market-events",
        client=sns_client,
    )
    unexpected_message: dict[str, Any] = {
        "price": 237.50,
        "unexpectedField": "unexpected-value",
    }

    with pytest.raises(ValueError, match="Unable to determine symbol"):
        event = quote_event_from_yfinance_message(unexpected_message)
        publisher.publish(event)

    sns_client.publish.assert_not_called()
