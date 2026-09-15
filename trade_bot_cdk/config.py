"""Deployment configuration shared by the CDK application and stacks."""

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
    """Resolve the context override or fall back to the stage default."""
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
