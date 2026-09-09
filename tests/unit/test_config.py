import pytest

from trade_bot_cdk.config import resolve_market_event_transport


@pytest.mark.parametrize(
    ("stage", "expected_transport"),
    [
        ("local", "log"),
        ("dev", "sns"),
        ("prod", "kinesis"),
    ],
)
def test_market_event_transport_uses_stage_default(
    stage: str,
    expected_transport: str,
) -> None:
    assert resolve_market_event_transport(stage) == expected_transport


def test_market_event_transport_context_override_wins() -> None:
    assert resolve_market_event_transport("dev", "kinesis") == "kinesis"


def test_market_event_transport_rejects_unsupported_override() -> None:
    with pytest.raises(ValueError, match="Unsupported marketEventTransport 'sqs'"):
        resolve_market_event_transport("dev", "sqs")
