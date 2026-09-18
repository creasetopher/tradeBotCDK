import pytest

from tradebot.brokers import LoggingOrderBroker

from order_execution_service import broker_factory
from order_execution_service.broker_factory import (
    BrokerMode,
    create_order_broker,
)


def test_creates_logging_broker() -> None:
    broker = create_order_broker(BrokerMode.LOG)

    assert isinstance(broker, LoggingOrderBroker)


def test_creates_alpaca_paper_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_broker = object()
    received_arguments = {}

    def create_fake_alpaca_broker(
        api_key: str,
        secret_key: str,
        *,
        paper: bool,
    ) -> object:
        received_arguments.update(
            {
                "api_key": api_key,
                "secret_key": secret_key,
                "paper": paper,
            }
        )
        return expected_broker

    monkeypatch.setattr(
        broker_factory,
        "AlpacaOrderBroker",
        create_fake_alpaca_broker,
    )

    broker = create_order_broker(
        BrokerMode.ALPACA_PAPER,
        alpaca_api_key="test-key",
        alpaca_secret_key="test-secret",
    )

    assert broker is expected_broker
    assert received_arguments == {
        "api_key": "test-key",
        "secret_key": "test-secret",
        "paper": True,
    }


@pytest.mark.parametrize(
    ("api_key", "secret_key"),
    [
        (None, "test-secret"),
        ("test-key", None),
        ("", "test-secret"),
        ("test-key", ""),
    ],
)
def test_alpaca_requires_both_credentials(
    api_key: str | None,
    secret_key: str | None,
) -> None:
    with pytest.raises(
        ValueError,
        match="Alpaca API key and secret key are required",
    ):
        create_order_broker(
            BrokerMode.ALPACA_PAPER,
            alpaca_api_key=api_key,
            alpaca_secret_key=secret_key,
        )

