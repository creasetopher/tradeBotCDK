import logging
from datetime import datetime, timezone
from decimal import Decimal

from tradebot.brokers import (
	OrderBroker,
	OrderRequest,
	OrderResult,
)

from .order_ddb_client import OrderDynamoDbClient
from .order_failures import (
	ExpectedOrderFailure,
	OrderFailure,
	OrderFailureCode,
	OrderFailureStage,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class OrderExecutionService:
	def __init__(
		self,
		broker: OrderBroker,
		order_ddb_client: OrderDynamoDbClient,
		*,
		max_order_quantity: Decimal = Decimal("100"),
	) -> None:
		"""Set the broker, order storage client, and maximum allowed order quantity."""
		self.broker = broker
		self.order_ddb_client = order_ddb_client
		self.max_order_quantity = max_order_quantity

	def execute(self, order: OrderRequest) -> OrderResult:
		"""Validate and submit an order, save the broker result, and return it.

		Save expected order failures before raising them for the caller to handle.
		Unexpected errors, including storage failures, propagate to the caller.
		"""
		try:
			self._validate_order(order)

			logger.info(
				"Submitting order client_order_id=%s symbol=%s quantity=%s, side=%s, type=%s",
				order.client_order_id,
				order.symbol,
				order.quantity,
				order.side.value,
				order.order_type.value,
			)

			result = self.broker.submit_order(order)
			self.order_ddb_client.save_order_result(result)

			logger.info(
				"Order submitted client_order_id=%s broker_order_id=%s status=%s",
				result.client_order_id,
				result.broker_order_id,
				result.status,
			)

			return result
		except ExpectedOrderFailure as error:
			self.order_ddb_client.save_order_failure(
				client_order_id=order.client_order_id,
				symbol=order.symbol,
				failure=error.failure,
			)
			raise

	def _validate_order(self, order: OrderRequest) -> None:

		"""Raise ExpectedOrderFailure if the order has expired or its quantity is invalid."""
		if datetime.now(timezone.utc) >= order.expires_at:
			raise ExpectedOrderFailure(
				OrderFailure(
					code=OrderFailureCode.EXPIRED,
					stage=OrderFailureStage.VALIDATION,
					reason=f"Order expired at {order.expires_at.isoformat()}",
				)
			)

		if order.quantity <= 0:
			raise ExpectedOrderFailure(
				OrderFailure(
					code=OrderFailureCode.INVALID_QUANTITY,
					stage=OrderFailureStage.VALIDATION,
					reason="Order quantity must be greater than zero",
				)
			)

		if order.quantity > self.max_order_quantity:
			raise ExpectedOrderFailure(
				OrderFailure(
					code=OrderFailureCode.QUANTITY_LIMIT_EXCEEDED,
					stage=OrderFailureStage.VALIDATION,
					reason=f"Order quantity cannot exceed {str(self.max_order_quantity)}",
				)
			)
