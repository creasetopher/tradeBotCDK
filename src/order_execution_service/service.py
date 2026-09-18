import logging
from decimal import Decimal

from tradebot.brokers import (
    OrderBroker,
    OrderRequest,
    OrderResult,
)

logger = logging.getLogger(__name__)


class OrderExecutionService:
    def __init__(
        self,
        broker: OrderBroker,
        *,
        max_order_quantity: Decimal = Decimal("100"),
    ) -> None:
        self.broker = broker
        self.max_order_quantity = max_order_quantity

    def execute(self, order: OrderRequest) -> OrderResult:
        self._validate_order(order)

        logger.info(
            "Submitting order client_order_id=%s symbol=%s quantity=%s",
            order.client_order_id,
            order.symbol,
            order.quantity,
        )

        result = self.broker.submit_order(order)

        logger.info(
            "Order submitted client_order_id=%s broker_order_id=%s status=%s",
            result.client_order_id,
            result.broker_order_id,
            result.status,
        )

        return result

    def _validate_order(self, order: OrderRequest) -> None:
        if order.quantity <= 0:
            raise ValueError("Order quantity must be greater than zero")

        if order.quantity > self.max_order_quantity:
            raise ValueError(
                f"Order quantity cannot exceed {self.max_order_quantity}"
            )