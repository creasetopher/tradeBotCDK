import logging
import os
from typing import Any

from tradebot.brokers import OrderBroker
from tradebot.models.order import OrderRequestMessage

from .broker_factory import BrokerMode, create_order_broker
from .order_ddb_client import OrderDynamoDbClient
from .order_failures import ExpectedOrderFailure
from .service import OrderExecutionService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _create_order_execution_service() -> OrderExecutionService:
	"""Build the order service using broker and table settings from the environment."""
	broker: OrderBroker = create_order_broker(
		BrokerMode(os.getenv("ORDER_BROKER_MODE", BrokerMode.LOG.value)),
		alpaca_api_key=os.getenv("ALPACA_API_KEY"),
		alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY"),
	)
	orders_table_name = os.environ.get("ORDERS_TABLE_NAME")
	if not orders_table_name:
		raise ValueError("ORDERS_TABLE_NAME environment variable is not set")
	ddb_order_client = OrderDynamoDbClient(orders_table_name)

	return OrderExecutionService(broker, ddb_order_client)


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
	"""Process SQS order messages and return the IDs of messages to retry.

	Expected order rejections have already been saved and are acknowledged.
	Parsing errors and unexpected processing errors are returned for retry.
	"""
	service = _create_order_execution_service()
	failures: list[dict[str, str]] = []

	for record in event["Records"]:
		try:
			order_request_message: OrderRequestMessage = (
				OrderRequestMessage.model_validate_json(record["body"])
			)
			service.execute(order_request_message.to_order_request())

		except ExpectedOrderFailure as error:
			# Do not add this message to batchItemFailures.
			# It was persisted and should not be retried.
			logger.warning(
				"Order rejected message_id=%s code=%s reason=%s",
				record.get("messageId"),
				error.failure.code.value,
				error.failure.reason,
			)

		except Exception:
			logger.exception(
				"Unexpected order-processing failure message_id=%s record=%s",
				record.get("messageId"),
				record,
			)
			failures.append({"itemIdentifier": record["messageId"]})

	return {"batchItemFailures": failures}
