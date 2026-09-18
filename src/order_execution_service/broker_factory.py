from enum import Enum

from tradebot.brokers import (
    AlpacaOrderBroker,
    LoggingOrderBroker,
    OrderBroker,
)


class BrokerMode(str, Enum):
    LOG = "log"
    ALPACA_PAPER = "alpaca_paper"


def create_order_broker(
    mode: BrokerMode,
    *,
    alpaca_api_key: str | None = None,
    alpaca_secret_key: str | None = None,
) -> OrderBroker:
    if mode is BrokerMode.LOG:
        return LoggingOrderBroker()

    if mode is BrokerMode.ALPACA_PAPER:
        if not alpaca_api_key or not alpaca_secret_key:
            raise ValueError(
                "Alpaca API key and secret key are required for paper trading"
            )

        return AlpacaOrderBroker(
            api_key=alpaca_api_key,
            secret_key=alpaca_secret_key,
            paper=True,
        )

    raise ValueError(f"Unsupported broker mode: {mode}")
