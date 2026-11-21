import pytest
from tradebot.models.transport import MarketEventTransport

from trade_bot_cdk.config import resolve_market_event_transport


@pytest.mark.parametrize(
	("stage", "expected_transport"),
	[
		("local", MarketEventTransport.LOG),
		("dev", MarketEventTransport.SNS),
		("prod", MarketEventTransport.KINESIS),
	],
)
def test_market_event_transport_uses_stage_default(
	stage: str,
	expected_transport: MarketEventTransport,
) -> None:
	assert resolve_market_event_transport(stage) == expected_transport


def test_market_event_transport_context_override_wins() -> None:
	assert (
		resolve_market_event_transport("dev", MarketEventTransport.KINESIS.value)
		== MarketEventTransport.KINESIS
	)


def test_market_event_transport_rejects_unsupported_override() -> None:
	with pytest.raises(ValueError, match="Unsupported marketEventTransport 'sqs'"):
		resolve_market_event_transport("dev", "sqs")
