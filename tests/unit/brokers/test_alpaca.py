from decimal import Decimal

import pytest
from alpaca.common.exceptions import APIError

from market_data_worker.brokers.alpaca import AlpacaOrderBroker
from market_data_worker.brokers.base import (
    OrderRequest,
    OrderSide,
    OrderSubmissionError,
    OrderType,
)


class RejectingAlpacaClient:
    def submit_order(self, *, order_data):
        raise APIError('{"code": 40310000, "message": "insufficient buying power"}')


def test_submit_order_translates_alpaca_rejection() -> None:
    broker = AlpacaOrderBroker("", "", client=RejectingAlpacaClient())
    order = OrderRequest(
        client_order_id="momentum-AAPL-123",
        symbol="AAPL",
        quantity=Decimal("10"),
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
    )

    with pytest.raises(OrderSubmissionError) as error:
        broker.submit_order(order)

    assert error.value.client_order_id == "momentum-AAPL-123"
    assert error.value.reason == (
        '{"code": 40310000, "message": "insufficient buying power"}'
    )
    assert isinstance(error.value.__cause__, APIError)
