"""Logging broker for local development and safe testing."""

from __future__ import annotations

import logging

from .base import OrderBroker, OrderRequest, OrderResult


class LoggingOrderBroker(OrderBroker):
    """Log orders without sending them to a real broker."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def submit_order(self, order: OrderRequest) -> OrderResult:
        """Log an order request and return a simulated result."""
        self._logger.info(
            "Simulated order: client_order_id=%s symbol=%s quantity=%s "
            "side=%s order_type=%s time_in_force=%s limit_price=%s",
            order.client_order_id,
            order.symbol,
            order.quantity,
            order.side.value,
            order.order_type,
            order.time_in_force.value,
            order.limit_price,
        )

        return OrderResult(
            broker_order_id=f"log-{order.client_order_id}",
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            status="logged",
        )
