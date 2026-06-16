from __future__ import annotations
from datetime import datetime, timezone
import logging
import os
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
import yfinance
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datetime import datetime, timezone
from decimal import Decimal

from tradebot.events.market import (
    MarketQuote,
    MarketQuoteEvent,
    build_raw_payload_hash,
    datetime_from_epoch_ms,
)

MARKET_EVENT_STREAM_NAME = os.getenv("MARKET_EVENT_STREAM_NAME")
ACTIVE_CANDIDATES_TABLE_NAME = os.getenv("ACTIVE_CANDIDATES_TABLE_NAME")
UNIVERSE_ID = os.environ.get("UNIVERSE_ID", "default_universe")

def quote_event_from_yfinance_message(message: dict) -> MarketQuoteEvent:
    collector_time = datetime.now(timezone.utc)

    source_ts_ms = message.get("time")
    event_time = (
        datetime_from_epoch_ms(source_ts_ms)
        if source_ts_ms is not None
        else collector_time
    )

    raw_payload_hash = build_raw_payload_hash(message)

    quote = MarketQuote(
        symbol=message["id"],
        event_time=event_time,
        collector_time=collector_time,
        price=_decimal_or_none(message.get("price")),
        day_volume=_decimal_or_none(message.get("dayVolume")),
        exchange=message.get("exchange"),
        market_hours=message.get("marketHours"),
        quote_type=message.get("quoteType"),
        source_payload_ts_ms=source_ts_ms,
        raw_payload_hash=raw_payload_hash,
    )

    return MarketQuoteEvent.from_quote(
        quote,
        source="yfinance.websocket",
        ingest_time=collector_time,
    )


def _decimal_or_none(value) -> Decimal | None:
    if value is None:
        return None

    return Decimal(str(value))

def get_active_candidates(table_name: str) -> list[str]:
    dynamodb_client = boto3.resource("dynamodb")
    table = dynamodb_client.Table(table_name)
    now = datetime.now(timezone.utc)

    try:
        response = table.query(
            KeyConditionExpression=Key("universe_id").eq(UNIVERSE_ID),
            FilterExpression=Attr("expires_at").gt(now.timestamp()),
        )
        return [item["symbol"] for item in response["Items"]]

    except Exception as e:
        logger.exception("Error querying active candidates from DynamoDB")
        raise e
    
async def subscribe_to_market_data_ws(symbols: list[str]) -> None:
    logger.info(f"Subscribing to market data stream for symbols: {symbols}")
    async with yfinance.AsyncWebSocket() as ws:
        await ws.subscribe(symbols)
        await ws.listen(message_handler=handle_market_data_message)
    # Implement your subscription logic here, e.g. connect to a WebSocket and subscribe to updates for the given symbols.

def handle_market_data_message(message: dict) -> None:
    market_quote_event: MarketQuoteEvent = quote_event_from_yfinance_message(message)
    logger.info(f"Received market quote event: {market_quote_event}")
    kinesis_client = boto3.client("kinesis")
    try:
        kinesis_client.put_record(
            StreamName=MARKET_EVENT_STREAM_NAME,
            PartitionKey=market_quote_event.symbol,
            Data=market_quote_event.model_dump_json(exclude_none=True).encode("utf-8"),
        )
    except Exception as e:
        logger.exception("Failed to put market quote event to Kinesis stream")
        raise e


async def main() -> None:
    logger.info("Market data worker started.")

    
    if not MARKET_EVENT_STREAM_NAME or not ACTIVE_CANDIDATES_TABLE_NAME:
        logger.error("One or more required environment variables are not set.")
        logger.error("MARKET_EVENT_STREAM_NAME=%s", MARKET_EVENT_STREAM_NAME)
        logger.error("ACTIVE_CANDIDATES_TABLE_NAME=%s", ACTIVE_CANDIDATES_TABLE_NAME)
        return

    active_candidates = get_active_candidates(ACTIVE_CANDIDATES_TABLE_NAME)
    await subscribe_to_market_data_ws(active_candidates)

    
    while True:
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
