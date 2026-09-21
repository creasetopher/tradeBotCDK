"""Live E2E test for the order-execution SQS pipeline.

OrderExecutionQueue -> Lambda -> LoggingOrderBroker -> OrdersTable
"""

import json
import os
import time
from typing import Any
from uuid import uuid4

import boto3
import pytest


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("RUN_E2E") != "1",
        reason="set RUN_E2E=1 to run against a deployed AWS stack",
    ),
]


POLL_INTERVAL_SECONDS = 2
DEFAULT_TIMEOUT_SECONDS = 60


def get_stack_outputs(
    cloudformation_client: Any,
    stack_name: str,
) -> dict[str, str]:
    response = cloudformation_client.describe_stacks(StackName=stack_name)
    outputs = response["Stacks"][0].get("Outputs", [])

    return {
        output["OutputKey"]: output["OutputValue"]
        for output in outputs
    }


def _wait_for_order_ddb_item(
    table: Any,
    client_order_id: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        response = table.get_item(
            Key={"client_order_id": client_order_id},
            ConsistentRead=True,
        )

        if "Item" in response:
            return response["Item"]

        time.sleep(POLL_INTERVAL_SECONDS)

    pytest.fail(
        f"Timed out waiting for order {client_order_id} DDB entry"
    )

def test_order_execution_pipeline() -> None:
    stage = os.environ.get("E2E_STAGE", "dev")
    stack_name = os.environ.get(
        "E2E_STACK_NAME",
        f"TradeBotCdkStack-{stage}",
    )
    timeout_seconds = int(
        os.environ.get(
            "E2E_TIMEOUT_SECONDS",
            DEFAULT_TIMEOUT_SECONDS,
        )
    )

    session = boto3.Session(
        region_name=os.environ.get("AWS_REGION")
    )
    cloudformation = session.client("cloudformation")
    sqs = session.client("sqs")
    dynamodb = session.resource("dynamodb")

    outputs = get_stack_outputs(cloudformation, stack_name)

    required_outputs = {
        "OrderExecutionQueueUrl",
        "OrdersTableName",
        "OrderBrokerMode",
    }
    missing = required_outputs - outputs.keys()
    assert not missing, (
        "Deploy the current stack; missing outputs: "
        f"{sorted(missing)}"
    )

    # This test must never submit a real order.
    assert outputs["OrderBrokerMode"] == "log"

    queue_url = outputs["OrderExecutionQueueUrl"]
    orders_table = dynamodb.Table(outputs["OrdersTableName"])

    client_order_id = f"e2e-aapl-{uuid4()}"
    order_request = {
        "client_order_id": client_order_id,
        "symbol": "AAPL",
        "quantity": "1",
        "side": "buy",
        "order_type": "market",
        "time_in_force": "day",
    }

    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(order_request),
        )

        assert response["MessageId"]

        stored_order = _wait_for_order_ddb_item(
            orders_table,
            client_order_id,
            timeout_seconds,
        )

        assert stored_order["client_order_id"] == client_order_id
        assert stored_order["broker_order_id"] == (
            f"log-{client_order_id}"
        )
        assert stored_order["symbol"] == "AAPL"
        assert stored_order["status"] == "logged"
        assert "recorded_at" in stored_order
    finally:
        orders_table.delete_item(
            Key={"client_order_id": client_order_id}
        )
