import json
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from lambda_functions.market_update_s3_writer import market_update_s3_writer as writer


@pytest.fixture
def mocked_aws(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_BUCKET_NAME", "test-market-data")
    monkeypatch.setenv("MARKET_EVENTS_TABLE_NAME", "test-market-events")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # sets the s3_client and table attributes of the writer module to None, ensuring that cached clients are not reused across tests
    monkeypatch.setattr(writer, "s3_client", None)
    monkeypatch.setattr(writer, "table", None)

    with mock_aws():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket="test-market-data")

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="test-market-events",
            KeySchema=[
                {"AttributeName": "symbol_day", "KeyType": "HASH"},
                {"AttributeName": "event_ts_ms", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "symbol_day", "AttributeType": "S"},
                {"AttributeName": "event_ts_ms", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # provide a mocked AWS environment with S3 and DynamoDB clients for each test
        yield {"s3_client": s3_client, "table": table, "handler": writer.handler}


def _sqs_event(
    body: str,
    message_id: str = "message-123",
    event_source: str = "aws:sqs",
) -> dict:
    return {
        "Records": [
            {
                "eventSource": event_source,
                "messageId": message_id,
                "body": body,
            }
        ]
    }


def test_handler_persists_sqs_market_event(mocked_aws) -> None:
    market_event = {
        "event_type": "market.quote.v1",
        "schema_version": "1.0.0",
        "source": "yfinance.websocket",
        "symbol": "AAPL",
        "event_time": "2026-09-09T15:30:00Z",
    }
    handler = mocked_aws.get("handler")
    result = handler(
        _sqs_event(json.dumps(market_event), event_source=" AWS:SQS "),
        SimpleNamespace(aws_request_id="request-123"),
    )

    assert result == {"batchItemFailures": []}

    s3_client = mocked_aws.get("s3_client")
    objects = s3_client.list_objects_v2(Bucket="test-market-data")["Contents"]
    assert len(objects) == 1
    stored_jsonl = s3_client.get_object(
        Bucket="test-market-data",
        Key=objects[0]["Key"],
    )["Body"].read().decode("utf-8")
    assert json.loads(stored_jsonl) == market_event

    items = mocked_aws.get("table").scan()["Items"]
    assert len(items) == 1
    assert items[0]["symbol"] == "AAPL"
    assert items[0]["event_type"] == "market.quote.v1"
    assert items[0]["s3_uri"] == (
        f"s3://test-market-data/{objects[0]['Key']}"
    )


def test_handler_reports_malformed_sqs_message(mocked_aws) -> None:
    handler = mocked_aws.get("handler")
    result = handler(
        _sqs_event("not valid JSON", message_id="bad-message"),
        SimpleNamespace(aws_request_id="request-123"),
    )

    assert result == {
        "batchItemFailures": [{"itemIdentifier": "bad-message"}]
    }


def test_handler_fails_fast_when_sqs_message_id_is_missing(mocked_aws) -> None:
    event = {
        "Records": [
            {
                "eventSource": "aws:sqs",
                "body": "{}",
            }
        ]
    }

    with pytest.raises(KeyError, match="messageId"):
        mocked_aws.get("handler")(
            event,
            SimpleNamespace(aws_request_id="request-123"),
        )
