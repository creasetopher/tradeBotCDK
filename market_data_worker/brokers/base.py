from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

"""Provider-neutral interface for broker integrations."""


class OrderSide(str, Enum):
    """Directions supported by our broker interface."""

    BUY = "buy"
    SELL = "sell"


class TimeInForce(str, Enum):
    """Order durations supported by our broker interface."""

    DAY = "day"
    GTC = "gtc"


class OrderType(str, Enum):
    """Order execution types supported by our broker interface."""

    MARKET = "market"
    LIMIT = "limit"


@dataclass(frozen=True)
class OrderRequest:
    """An order our trade bot wants a broker to execute."""

    client_order_id: str
    symbol: str
    quantity: Decimal
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Decimal | None = None


@dataclass(frozen=True)
class OrderResult:
    """Provider-neutral response from a broker."""

    broker_order_id: str
    client_order_id: str
    symbol: str
    status: str


class OrderSubmissionError(Exception):
    """Raised when a broker rejects an order submission."""

    def __init__(self, client_order_id: str, reason: str) -> None:
        self.client_order_id = client_order_id
        self.reason = reason
        super().__init__(
            f"Order {client_order_id!r} could not be submitted: {reason}"
        )


class OrderBroker(ABC):
    """Interface implemented by paper, logging, and live brokers."""

    @abstractmethod
    def submit_order(self, order: OrderRequest) -> OrderResult:
        """Submit an order and return the broker's response."""
        ...
