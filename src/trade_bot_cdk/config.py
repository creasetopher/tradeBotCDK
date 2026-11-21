"""Stage-specific CDK configuration."""

from typing import Literal

from tradebot.models.transport import MarketEventTransport

MARKET_EVENT_TRANSPORT_BY_STAGE = {
	"local": MarketEventTransport.LOG,
	"dev": MarketEventTransport.SNS,
	"prod": MarketEventTransport.KINESIS,
}


def resolve_market_event_transport(
	stage: str,
	context_override: str | None = None,
) -> MarketEventTransport:
	"""Choose the transport override, or use the default for the deployment stage."""
	transport_value = (
		context_override.strip().lower()
		if context_override
		else MARKET_EVENT_TRANSPORT_BY_STAGE.get(
			stage.lower(),
			MarketEventTransport.SNS,
		)
	)
	try:
		return MarketEventTransport(transport_value)
	except ValueError as error:
		supported = ", ".join(transport.value for transport in MarketEventTransport)
		raise ValueError(
			f"Unsupported marketEventTransport '{transport_value}'. "
			f"Expected one of: {supported}"
		) from error


def resolve_scanner_state(
	stage: str, context_override: Literal["0", "1"] | None = None
) -> bool:
	"""Return whether the scanner should run for this deployment.

	An override of "1" enables it and "0" disables it. Without an override,
	enable it only in prod. Raise ValueError for any other override.
	"""
	if context_override is None:
		return stage.lower() == "prod"

	if context_override not in ("0", "1"):
		raise ValueError(
			f"Unsupported scanner state '{context_override}'. Expected one of: {0, 1}"
		)

	return int(context_override) == 1
