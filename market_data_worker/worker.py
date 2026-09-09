from __future__ import annotations
from typing import Any
from datetime import datetime, timezone
import logging
import os
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
import yfinance
import asyncio

try:
    from .publishers import (
        KinesisMarketEventPublisher,
        LoggingMarketEventPublisher,
        MarketEventPublisher,
        SnsMarketEventPublisher,
    )
except ImportError:  # worker.py is executed directly inside the container, this can happen when running the script directly for testing or debugging
    from publishers import (
        KinesisMarketEventPublisher,
        LoggingMarketEventPublisher,
        MarketEventPublisher,
        SnsMarketEventPublisher,
    )

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datetime import datetime, timezone
from decimal import Decimal

from tradebot.events.market import (
    MarketQuoteEvent,
)

from tradebot.providers.yfinance import quote_event_from_yfinance_message

MARKET_EVENT_STREAM_NAME = os.getenv("MARKET_EVENT_STREAM_NAME")
MARKET_EVENT_TOPIC_ARN = os.getenv("MARKET_EVENT_TOPIC_ARN")
MARKET_EVENT_TRANSPORT = os.getenv("MARKET_EVENT_TRANSPORT", "kinesis").lower()
ACTIVE_CANDIDATES_TABLE_NAME = os.getenv("ACTIVE_CANDIDATES_TABLE_NAME")
UNIVERSE_ID = os.environ.get("UNIVERSE_ID", "default_universe")
REFRESH_INTERVAL_SECONDS = int(os.getenv("REFRESH_INTERVAL_SECONDS", "300"))


dynamodb_client = boto3.resource("dynamodb")
market_event_publisher: MarketEventPublisher | None = None

# def _symbol_from_message(message: dict[str, Any]) -> str:
#     symbol = message.get("id") or message.get("symbol")
#     if not symbol:
#         raise ValueError(f"Unable to determine symbol from message keys={list(message.keys())}")
#     return str(symbol).upper()

# def quote_event_from_yfinance_message(message: dict) -> MarketQuoteEvent:
#     collector_time = datetime.now(timezone.utc)

#     source_ts_ms = message.get("time")
#     event_time = (
#         datetime_from_epoch_ms(source_ts_ms)
#         if source_ts_ms is not None
#         else collector_time
#     )

#     raw_payload_hash = build_raw_payload_hash(message)

#     quote = MarketQuote(
#         symbol=_symbol_from_message(message),
#         event_time=event_time,
#         collector_time=collector_time,
#         price=_decimal_or_none(message.get("price")),
#         day_volume=_decimal_or_none(message.get("dayVolume")),
#         exchange=message.get("exchange"),
#         market_hours=message.get("marketHours"),
#         quote_type=message.get("quoteType"),
#         source_payload_ts_ms=source_ts_ms,
#         raw_payload_hash=raw_payload_hash,
#     )

#     return MarketQuoteEvent.from_quote(
#         quote,
#         source="yfinance.websocket",
#         ingest_time=collector_time,
#     )


# def _decimal_or_none(value) -> Decimal | None:
#     if value is None:
#         return None

#     return Decimal(str(value))

def get_active_candidates(table_name: str) -> set[str]:
    table = dynamodb_client.Table(table_name)
    now_epoch = int(time.time())

    query_params = {
        "KeyConditionExpression": Key("universe_id").eq(UNIVERSE_ID),
        "FilterExpression": Attr("expires_at").gt(now_epoch),
        "ProjectionExpression": "symbol, expires_at",
    }
    try:
        symbols: set[str] = set()
        while True:    
            response = table.query(**query_params)

            for item in response.get("Items", []):
                symbol = str(item["symbol"]).strip().upper()
                if symbol:
                    symbols.add(symbol)

            # check if there are more pages of results
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
            query_params["ExclusiveStartKey"] = last_evaluated_key

        return symbols

    except Exception as e:
        logger.exception("Error querying active candidates from DynamoDB")
        raise e
    

def create_market_event_publisher(
    transport: str,
    *,
    stream_name: str | None = None,
    topic_arn: str | None = None,
) -> MarketEventPublisher:
    """Create the publisher configured for this worker process."""
    normalized_transport = transport.strip().lower()
    if normalized_transport == "log":
        return LoggingMarketEventPublisher(logger)
    if normalized_transport == "kinesis":
        if not stream_name:
            raise RuntimeError("MARKET_EVENT_STREAM_NAME is required for kinesis")
        return KinesisMarketEventPublisher(stream_name)
    if normalized_transport == "sns":
        if not topic_arn:
            raise RuntimeError("MARKET_EVENT_TOPIC_ARN is required for sns")
        return SnsMarketEventPublisher(topic_arn)
    raise ValueError(f"Unsupported MARKET_EVENT_TRANSPORT: {transport}")


def handle_market_data_message(
    message: dict,
    publisher: MarketEventPublisher | None = None,
) -> None:
    """Convert a provider message and publish the resulting domain event."""
    market_quote_event: MarketQuoteEvent = quote_event_from_yfinance_message(message)
    logger.info(f"Received market quote event: {market_quote_event}")
    selected_publisher = publisher or market_event_publisher
    if selected_publisher is None:
        raise RuntimeError("Market event publisher has not been configured")
    try:
        selected_publisher.publish(market_quote_event)
    except Exception as e:
        logger.exception(
            "Failed to publish market quote event\nmessage: %s\nerror: %s",
            market_quote_event,
            e,
        )

# keep in mind two web socket processes are running in parallel (listen and subscribe/unsubscribe), 
# and there may be some overlap in messages received during subscription changes,
# so the market data worker may receive duplicate messages for the same symbol. 
# The worker should be designed to handle this gracefully, and downstream 
# processing should be idempotent to avoid issues with duplicates.
async def run_websocket_session() -> None:
    subscribed_symbols: set[str] = set()

    async with yfinance.AsyncWebSocket() as ws:
        listener_task = asyncio.create_task(
            ws.listen(message_handler=handle_market_data_message)
        )

        try:
            while True:
                if not ACTIVE_CANDIDATES_TABLE_NAME:
                    logger.error("ACTIVE_CANDIDATES_TABLE_NAME is not set")
                    raise RuntimeError("ACTIVE_CANDIDATES_TABLE_NAME is not set")

                active_symbols = set(get_active_candidates(ACTIVE_CANDIDATES_TABLE_NAME))

                symbols_to_add = active_symbols - subscribed_symbols
                symbols_to_remove = subscribed_symbols - active_symbols

                if symbols_to_add:
                    logger.info("Subscribing to symbols: %s", sorted(symbols_to_add))
                    await ws.subscribe(sorted(symbols_to_add))

                if symbols_to_remove:
                    logger.info("Unsubscribing from symbols: %s", sorted(symbols_to_remove))
                    await ws.unsubscribe(sorted(symbols_to_remove))

                subscribed_symbols = active_symbols

                # check if listener task has exited unexpectedly
                if listener_task.done():
                    exc = listener_task.exception()
                    if exc:
                        raise exc
                    raise RuntimeError("WebSocket listener exited unexpectedly")

                logger.info(
                    "Active universe refreshed: active=%d subscribed=%d",
                    len(active_symbols),
                    len(subscribed_symbols),
                )

                await asyncio.sleep(REFRESH_INTERVAL_SECONDS)

        finally:
            listener_task.cancel()
            try:
                await listener_task
            except asyncio.CancelledError:
                pass

async def run_market_data_worker() -> None:
    logger.info("Market data worker started.")
    backoff_retry_seconds = 5
    while True:
        try:
            await run_websocket_session()
            backoff_retry_seconds = 5

        except Exception:
            logger.exception(
                "Market data WebSocket session failed; reconnecting in %s seconds",
                backoff_retry_seconds,
            )
            await asyncio.sleep(backoff_retry_seconds)
            # Double the backoff time for the next retry, up to a maximum of 5 minutes.
            backoff_retry_seconds = min(backoff_retry_seconds * 2, 300)

async def main() -> None:
    global market_event_publisher

    if not ACTIVE_CANDIDATES_TABLE_NAME:
        raise RuntimeError("ACTIVE_CANDIDATES_TABLE_NAME is required")

    market_event_publisher = create_market_event_publisher(
        MARKET_EVENT_TRANSPORT,
        stream_name=MARKET_EVENT_STREAM_NAME,
        topic_arn=MARKET_EVENT_TOPIC_ARN,
    )

    await run_market_data_worker()


if __name__ == "__main__":
    asyncio.run(main())
