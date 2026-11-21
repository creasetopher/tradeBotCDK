from datetime import datetime, timezone
from typing import Any

import boto3
from tradebot.brokers import OrderResult

from .order_failures import OrderFailure


class OrderDynamoDbClient:
	def __init__(
		self,
		table_name: str,
		*,
		dynamodb_resource: Any | None = None,
	) -> None:
		"""Connect to the orders table using the supplied resource or a new one."""
		dynamodb = dynamodb_resource or boto3.resource("dynamodb")
		self.table = dynamodb.Table(table_name)

	def save_order_result(self, order_result: OrderResult) -> None:
		"""Save the broker result, replacing any record with the same client order ID."""
		self.table.put_item(
			Item={
				"client_order_id": order_result.client_order_id,
				"broker_order_id": order_result.broker_order_id,
				"symbol": order_result.symbol,
				"status": order_result.status,
				"recorded_at": datetime.now(timezone.utc).isoformat(),
			}
		)

	def save_order_failure(
		self,
		*,
		client_order_id: str,
		symbol: str,
		failure: OrderFailure,
	) -> None:
		"""Save the failure details and timestamp under the client order ID."""
		self.table.put_item(
			Item={
				"client_order_id": client_order_id,
				"symbol": symbol,
				"status": "failed",
				"failure_code": failure.code.value,
				"failure_stage": failure.stage.value,
				"failure_reason": failure.reason,
				"recorded_at": datetime.now(timezone.utc).isoformat(),
			}
		)
