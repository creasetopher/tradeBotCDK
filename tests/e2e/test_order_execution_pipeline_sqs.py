"""Live E2E test for the order-execution SQS pipeline.

OrderExecutionQueue -> Lambda -> LoggingOrderBroker -> OrdersTable
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
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

	return {output["OutputKey"]: output["OutputValue"] for output in outputs}


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

	pytest.fail(f"Timed out waiting for order {client_order_id} DDB entry")


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

	session = boto3.Session(region_name=os.environ.get("AWS_REGION"))
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
	assert not missing, f"Deploy the current stack; missing outputs: {sorted(missing)}"

	# This test must never submit a real order.
	assert outputs["OrderBrokerMode"] == "log"

	queue_url = outputs["OrderExecutionQueueUrl"]
	orders_table = dynamodb.Table(outputs["OrdersTableName"])

	client_order_id = f"e2e-aapl-{uuid4()}"
	expired_client_order_id = f"e2e-expired-aapl-{uuid4()}"

	created_at = datetime.now(timezone.utc)
	expires_at = created_at + timedelta(seconds=30)
	order_request = {
		"event_type": "order.request.v1",
		"schema_version": "1.0.0",
		"client_order_id": client_order_id,
		"symbol": "AAPL",
		"quantity": "1",
		"side": "buy",
		"order_type": "market",
		"time_in_force": "day",
		"created_at": created_at.isoformat(),
		"expires_at": expires_at.isoformat(),
	}
	expired_created_at = created_at - timedelta(minutes=2)
	expired_at = created_at - timedelta(minutes=1)
	expired_order_request = {
		**order_request,
		"client_order_id": expired_client_order_id,
		"created_at": expired_created_at.isoformat(),
		"expires_at": expired_at.isoformat(),
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
		assert stored_order["broker_order_id"] == (f"log-{client_order_id}")
		assert stored_order["symbol"] == "AAPL"
		assert stored_order["status"] == "logged"
		assert "recorded_at" in stored_order

        # test the flow of an expired order,
		# the execution service will consume the 
		# order from SQS, check it's expirartion
		# and if expired, not execute it and store
		# the expired/failed order in DDB
		expired_response = sqs.send_message(
			QueueUrl=queue_url,
			MessageBody=json.dumps(expired_order_request),
		)

		assert expired_response["MessageId"]

		stored_expired_order = _wait_for_order_ddb_item(
			orders_table,
			expired_client_order_id,
			timeout_seconds,
		)

		assert stored_expired_order["client_order_id"] == expired_client_order_id
		assert stored_expired_order["symbol"] == "AAPL"
		assert stored_expired_order["status"] == "failed"
		assert stored_expired_order["failure_code"] == "expired"
		assert stored_expired_order["failure_stage"] == "validation"
		assert stored_expired_order["failure_reason"] == (
			f"Order expired at {expired_at.isoformat()}"
		)
		assert "recorded_at" in stored_expired_order
	finally:
		orders_table.delete_item(Key={"client_order_id": client_order_id})
		orders_table.delete_item(Key={"client_order_id": expired_client_order_id})
