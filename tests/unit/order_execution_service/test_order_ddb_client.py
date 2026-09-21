from datetime import datetime, timedelta

import boto3
import pytest
from moto import mock_aws

from tradebot.brokers import OrderResult

from order_execution_service.order_ddb_client import OrderDynamoDbClient



@pytest.fixture
def mocked_order_table(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="test-orders",
            KeySchema=[
                {"AttributeName": "client_order_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "client_order_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield table


def test_saves_order_result(mocked_order_table) -> None:
    client = OrderDynamoDbClient(
        mocked_order_table.name,
    )
    result = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id="momentum-aapl-123",
        symbol="AAPL",
        status="accepted",
    )

    client.save_order_result(result)

    item = mocked_order_table.get_item(
        Key={"client_order_id": "momentum-aapl-123"}
    )["Item"]

    assert item["client_order_id"] == "momentum-aapl-123"
    assert item["broker_order_id"] == "alpaca-order-456"
    assert item["symbol"] == "AAPL"
    assert item["status"] == "accepted"
    recorded_at = datetime.fromisoformat(item["recorded_at"])
    assert recorded_at.utcoffset() == timedelta(0)

# test that saving an order result with the same client_order_id replaces the existing record in DynamoDB
def test_replaces_result_with_same_client_order_id(mocked_order_table) -> None:
    client = OrderDynamoDbClient(
        mocked_order_table.name,
    )
    accepted_result = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id="momentum-aapl-123",
        symbol="AAPL",
        status="accepted",
    )
    filled_result = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id="momentum-aapl-123",
        symbol="AAPL",
        status="filled",
    )

    client.save_order_result(accepted_result)
    client.save_order_result(filled_result)

    items = mocked_order_table.scan()["Items"]

    assert len(items) == 1
    assert items[0]["status"] == "filled"


def test_result_of_two_different_orders_same_symbol(mocked_order_table) -> None:
    client = OrderDynamoDbClient(
        mocked_order_table.name,
    )
    filled_result1 = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id="momentum-aapl-123",
        symbol="AAPL",
        status="filled",
    )
    filled_result2 = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id="obs-aapl-789",
        symbol="AAPL",
        status="filled",
    )

    client.save_order_result(filled_result1)
    client.save_order_result(filled_result2)

    items = mocked_order_table.scan()["Items"]

    assert len(items) == 2
    assert items[0]["status"] == "filled"
    assert items[1]["status"] == "filled"
    assert items[0]["client_order_id"] != items[1]["client_order_id"]