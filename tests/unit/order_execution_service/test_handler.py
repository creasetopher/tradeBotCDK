import pytest
import json
from typing import Iterator
from decimal import Decimal
from unittest.mock import Mock

from tradebot.brokers import (
    OrderSide,
    OrderType,
    TimeInForce,
    OrderRequest,
)

from order_execution_service import handler as order_handler
from order_execution_service.service import OrderExecutionService


@pytest.fixture
def mock_service(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Mock]:
    service = Mock(spec=OrderExecutionService)

    monkeypatch.setattr(
        order_handler,
        "_create_order_execution_service",
        lambda: service,
    )

    yield service


def _sqs_record(message_id: str, body: dict[str, str]) -> dict:
    return {
        "messageId": message_id,
        "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
        "body": json.dumps(body),
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1789769462000",
            "SenderId": "AROAXXXXXXXXXXXXXXXXX",
            "ApproximateFirstReceiveTimestamp": "1789769462100",
        },
        "messageAttributes": {},
        "md5OfBody": "4c92d6e8f24c42f3a50c92caa31e4916",
        "eventSource": "aws:sqs",
        "eventSourceARN": (
            "arn:aws:sqs:us-east-2:123456789012:"
            "tradebot-dev-order-execution"
        ),
        "awsRegion": "us-east-2",
    }


def test_handler_parses_and_executes_market_order(mock_service: Mock) -> None:
    event = {
        "Records": [
            _sqs_record(
                "message-123",
                {
                    "client_order_id": "momentum-aapl-123",
                    "symbol": "AAPL",
                    "quantity": "5",
                    "side": "buy",
                    "order_type": "market",
                },
            )
        ]
    }

    response = order_handler.handler(event, None)

    assert response == {"batchItemFailures": []}

    mock_service.execute.assert_called_once()
    order: OrderRequest = mock_service.execute.call_args.args[0]
    assert isinstance(order, OrderRequest)
    assert order.client_order_id == "momentum-aapl-123"
    assert order.symbol == "AAPL"
    assert order.quantity == Decimal("5")
    assert order.side is OrderSide.BUY
    assert order.order_type is OrderType.MARKET
    assert order.time_in_force is TimeInForce.DAY
    assert order.limit_price is None

def test_handler_parses_and_executes_limit_order(mock_service: Mock) -> None:
    event = {
        "Records": [
            _sqs_record(
                "message-456",
                {
                    "client_order_id": "momentum-nvda-456",
                    "symbol": "NVDA",
                    "quantity": "2",
                    "side": "sell",
                    "order_type": "limit",
                    "time_in_force": "gtc",
                    "limit_price": "300.00",
                },
            )
        ]
    }

    response = order_handler.handler(event, None)

    assert response == {"batchItemFailures": []}

    mock_service.execute.assert_called_once()
    order: OrderRequest = mock_service.execute.call_args.args[0]
    assert isinstance(order, OrderRequest)
    assert order.client_order_id == "momentum-nvda-456"
    assert order.symbol == "NVDA"
    assert order.quantity == Decimal("2")
    assert order.side is OrderSide.SELL
    assert order.order_type is OrderType.LIMIT
    assert order.time_in_force is TimeInForce.GTC
    assert order.limit_price == Decimal("300.00")

def test_handler_reports_only_failed_messages(mock_service: Mock) -> None:
    mock_service.execute.side_effect = [
        None,
        ValueError("Order rejected"),
    ]

    event = {
        "Records": [
            _sqs_record(
                "successful-message",
                {
                    "client_order_id": "momentum-aapl-123",
                    "symbol": "AAPL",
                    "quantity": "5",
                    "side": "buy",
                    "order_type": "market",
                },
            ),
            _sqs_record(
                "failed-message",
                {
                    "client_order_id": "momentum-nvda-456",
                    "symbol": "NVDA",
                    "quantity": "2",
                    "side": "buy",
                    "order_type": "market",
                },
            ),
        ]
    }

    response = order_handler.handler(event, None)

    assert response == {
        "batchItemFailures": [
            {"itemIdentifier": "failed-message"},
        ]
    }
    assert mock_service.execute.call_count == 2


def test_handler_reports_invalid_json_as_failure(mock_service: Mock) -> None:
    event = {
        "Records": [
            {
                "messageId": "corrupted-message",
                "body": "this is not JSON",
                "eventSource": "aws:sqs",
            }
        ]
    }

    response = order_handler.handler(event, None)

    assert response == {
        "batchItemFailures": [
            {"itemIdentifier": "corrupted-message"},
        ]
    }
    mock_service.execute.assert_not_called()
