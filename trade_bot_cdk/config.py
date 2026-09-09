"""Deployment configuration shared by the CDK application and stacks."""

SUPPORTED_MARKET_EVENT_TRANSPORTS = {"kinesis", "log", "sns"}

MARKET_EVENT_TRANSPORT_BY_STAGE = {
    "local": "log",
    "dev": "sns",
    "prod": "kinesis",
}


def resolve_market_event_transport(
    stage: str,
    context_override: str | None = None,
) -> str:
    """Resolve the context override or fall back to the stage default."""
    transport = (
        context_override.strip().lower()
        if context_override
        else MARKET_EVENT_TRANSPORT_BY_STAGE.get(stage.lower(), "sns")
    )
    if transport not in SUPPORTED_MARKET_EVENT_TRANSPORTS:
        supported = ", ".join(sorted(SUPPORTED_MARKET_EVENT_TRANSPORTS))
        raise ValueError(
            f"Unsupported marketEventTransport '{transport}'. "
            f"Expected one of: {supported}"
        )
    return transport
