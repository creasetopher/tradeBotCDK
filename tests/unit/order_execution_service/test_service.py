from datetime import datetime, timedelta, timezone
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

from order_execution_service.order_ddb_client import OrderDynamoDbClient
from order_execution_service.order_failures import (
	ExpectedOrderFailure,
	OrderFailureCode,
	OrderFailureStage,
)
from order_execution_service.service import OrderExecutionService


def build_order_request(
	quantity: str = "5",
	expires_at: datetime | None = None,
) -> OrderRequest:
	expiration_time = expires_at or (datetime.now(timezone.utc) + timedelta(seconds=30))
	return OrderRequest(
		client_order_id="momentum-aapl-123",
		symbol="AAPL",
		quantity=Decimal(quantity),
		side=OrderSide.BUY,
		order_type=OrderType.MARKET,
		time_in_force=TimeInForce.DAY,
		expires_at=expiration_time,
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
	mock_ddb_client = Mock(spec=OrderDynamoDbClient)
	broker.submit_order.return_value = expected_result

	service = OrderExecutionService(broker, mock_ddb_client)

	result = service.execute(order)

	assert result == expected_result
	broker.submit_order.assert_called_once_with(order)
	mock_ddb_client.save_order_result.assert_called_once_with(expected_result)


@pytest.mark.parametrize("quantity", ["0", "-1"])
def test_rejects_non_positive_quantity(quantity: str) -> None:
	broker = Mock(spec=OrderBroker)
	order = build_order_request(quantity)
	mock_ddb_client = Mock(spec=OrderDynamoDbClient)
	service = OrderExecutionService(broker, mock_ddb_client)

	with pytest.raises(
		ExpectedOrderFailure,
		match="Order quantity must be greater than zero",
	) as error:
		service.execute(order)
	mock_ddb_client.save_order_failure.assert_called_once_with(
		client_order_id=order.client_order_id,
		symbol=order.symbol,
		failure=error.value.failure,
	)
	broker.submit_order.assert_not_called()
	mock_ddb_client.save_order_result.assert_not_called()
	assert error.value.failure.code is OrderFailureCode.INVALID_QUANTITY
	assert error.value.failure.stage is OrderFailureStage.VALIDATION


def test_rejects_expired_order() -> None:
	broker = Mock(spec=OrderBroker)
	mock_ddb_client = Mock(spec=OrderDynamoDbClient)
	expired_at = datetime.now(timezone.utc) - timedelta(seconds=1)
	order = build_order_request(expires_at=expired_at)
	service = OrderExecutionService(broker, mock_ddb_client)

	with pytest.raises(ExpectedOrderFailure) as error:
		service.execute(order)

	assert error.value.failure.code is OrderFailureCode.EXPIRED
	assert error.value.failure.stage is OrderFailureStage.VALIDATION
	assert error.value.failure.reason == (f"Order expired at {expired_at.isoformat()}")
	mock_ddb_client.save_order_failure.assert_called_once_with(
		client_order_id=order.client_order_id,
		symbol=order.symbol,
		failure=error.value.failure,
	)
	broker.submit_order.assert_not_called()
	mock_ddb_client.save_order_result.assert_not_called()


def test_order_failure_persistence_error_propagates() -> None:
	broker = Mock(spec=OrderBroker)
	mock_ddb_client = Mock(spec=OrderDynamoDbClient)
	mock_ddb_client.save_order_failure.side_effect = RuntimeError(
		"DynamoDB unavailable"
	)
	order = build_order_request(
		expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
	)
	service = OrderExecutionService(broker, mock_ddb_client)

	with pytest.raises(RuntimeError, match="DynamoDB unavailable"):
		service.execute(order)

	mock_ddb_client.save_order_failure.assert_called_once()
	broker.submit_order.assert_not_called()
	mock_ddb_client.save_order_result.assert_not_called()


def test_rejects_quantity_above_maximum() -> None:
	"""Check that the service rejects quantities above its configured maximum."""
	broker = Mock(spec=OrderBroker)
	order = build_order_request("101")
	mock_ddb_client = Mock(spec=OrderDynamoDbClient)
	service = OrderExecutionService(
		broker,
		mock_ddb_client,
		max_order_quantity=Decimal("100"),
	)

	with pytest.raises(
		ExpectedOrderFailure,
	) as error:
		service.execute(order)

	assert error.value.failure.code is OrderFailureCode.QUANTITY_LIMIT_EXCEEDED
	assert error.value.failure.stage is OrderFailureStage.VALIDATION
	assert error.value.failure.reason == ("Order quantity cannot exceed 100")
	mock_ddb_client.save_order_failure.assert_called_once_with(
		client_order_id=order.client_order_id,
		symbol=order.symbol,
		failure=error.value.failure,
	)
	broker.submit_order.assert_not_called()
	mock_ddb_client.save_order_result.assert_not_called()
