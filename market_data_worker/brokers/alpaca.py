"""Alpaca implementation of the order broker interface."""

from __future__ import annotations

from typing import Any

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide as AlpacaOrderSide
from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

from .base import (
    OrderBroker,
    OrderRequest,
    OrderResult,
    OrderSubmissionError,
    OrderType,
)


class AlpacaOrderBroker(OrderBroker):
    """Submit provider-neutral orders through Alpaca's Trading API."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        *,
        paper: bool = True,
        client: Any | None = None,
    ) -> None:
        if client is None and (not api_key or not secret_key):
            raise ValueError("Alpaca API key and secret key are required")

        self._client = client or TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
        )


    def submit_order(self, order: OrderRequest) -> OrderResult:
        """Translate, submit, and normalize an Alpaca order.
            Submits an order to Alpaca's Trading API and returns a provider-neutral OrderResult.
            The response from Alpaca is normalized to the OrderResult dataclass, which includes the broker_order_id, client_order_id, symbol, and status.
            The resoponse from Alpaca might look like this:
            {
                "id": "61e69015-8549-4bfd-b9c3-01e75843f47d",
                "client_order_id": "momentum-aapl-123",
                "symbol": "AAPL",
                "qty": "5",
                "side": "buy",
                "type": "market",
                "status": OrderStatus.ACCEPTED
                "submitted_at": "2026-09-11T15:31:02Z"
            }
        """
        request: MarketOrderRequest | LimitOrderRequest = self._build_order_request(order)
        try:
            alpaca_order = self._client.submit_order(order_data=request)
        except APIError as error:
            raise OrderSubmissionError(
                client_order_id=order.client_order_id,
                reason=str(error),
            ) from error

        return OrderResult(
            broker_order_id=str(alpaca_order.id),
            client_order_id=alpaca_order.client_order_id,
            symbol=alpaca_order.symbol or order.symbol,
            status=alpaca_order.status.value,
        )

    @staticmethod
    def _build_order_request(
        order: OrderRequest,
    ) -> MarketOrderRequest | LimitOrderRequest:
        common_fields = {
            "symbol": order.symbol.upper(),
            "qty": float(order.quantity),
            "side": AlpacaOrderSide(order.side),
            "time_in_force": AlpacaTimeInForce(order.time_in_force),
            "client_order_id": order.client_order_id,
        }

        if order.order_type == OrderType.MARKET:
            return MarketOrderRequest(**common_fields)

        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None or order.limit_price <= 0:
                raise ValueError(f"limit_price is required for limit orders. limit price: {order.limit_price}")
            return LimitOrderRequest(
                **common_fields,
                limit_price=float(order.limit_price),
            )

        raise ValueError(f"Unsupported order type: {order.order_type}")
