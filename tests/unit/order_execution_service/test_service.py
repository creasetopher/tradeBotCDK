from decimal import Decimal
from unittest.mock import Mock

import pytest

from tradebot.brokers import (
    OrderBroker,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderType,
    TimeInForce,
)

from order_execution_service.service import OrderExecutionService


def build_order_request(quantity: str = "5") -> OrderRequest:
    return OrderRequest(
        client_order_id="momentum-aapl-123",
        symbol="AAPL",
        quantity=Decimal(quantity),
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
    )


def test_executes_order_using_broker() -> None:
    broker = Mock(spec=OrderBroker)
    order = build_order_request()
    expected_result = OrderResult(
        broker_order_id="alpaca-order-456",
        client_order_id=order.client_order_id,
        symbol=order.symbol,
        status="accepted",
    )
    broker.submit_order.return_value = expected_result

    service = OrderExecutionService(broker)

    result = service.execute(order)

    assert result == expected_result
    broker.submit_order.assert_called_once_with(order)


@pytest.mark.parametrize("quantity", ["0", "-1"])
def test_rejects_non_positive_quantity(quantity: str) -> None:
    broker = Mock(spec=OrderBroker)
    order = build_order_request(quantity)
    service = OrderExecutionService(broker)

    with pytest.raises(
        ValueError,
        match="Order quantity must be greater than zero",
    ):
        service.execute(order)

    broker.submit_order.assert_not_called()

# test that the service rejects orders with quantity above the maximum allowed quantity
# and respects the max_order_quantity parameter passed to the service
def test_rejects_quantity_above_maximum() -> None:
    broker = Mock(spec=OrderBroker)
    order = build_order_request("101")
    service = OrderExecutionService(
        broker,
        max_order_quantity=Decimal("100"),
    )

    with pytest.raises(
        ValueError,
        match="Order quantity cannot exceed 100",
    ):
        service.execute(order)

    broker.submit_order.assert_not_called()
