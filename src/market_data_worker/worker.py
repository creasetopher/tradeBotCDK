"""Continuously collect and publish market quote events."""

from __future__ import annotations

import asyncio
import logging
import os
import time

import boto3
import yfinance
from boto3.dynamodb.conditions import Attr, Key
from tradebot.events.market import (
	MarketQuoteEvent,
)
from tradebot.models.transport import MarketEventTransport
from tradebot.providers.yfinance import quote_event_from_yfinance_message

try:
	from .publishers import (
		KinesisMarketEventPublisher,
		LoggingMarketEventPublisher,
		MarketEventPublisher,
		SnsMarketEventPublisher,
	)
	# worker.py is executed directly inside the container,
	# this can happen when running the script directly for testing or debugging
except ImportError:
	from publishers import (
		KinesisMarketEventPublisher,
		LoggingMarketEventPublisher,
		MarketEventPublisher,
		SnsMarketEventPublisher,
	)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


MARKET_EVENT_STREAM_NAME = os.getenv("MARKET_EVENT_STREAM_NAME")
MARKET_EVENT_TOPIC_ARN = os.getenv("MARKET_EVENT_TOPIC_ARN")
MARKET_EVENT_TRANSPORT = os.getenv(
	"MARKET_EVENT_TRANSPORT",
	MarketEventTransport.KINESIS.value,
)
ACTIVE_CANDIDATES_TABLE_NAME = os.getenv("ACTIVE_CANDIDATES_TABLE_NAME")
UNIVERSE_ID = os.environ.get("UNIVERSE_ID", "default_universe")
REFRESH_INTERVAL_SECONDS = int(os.getenv("REFRESH_INTERVAL_SECONDS", "300"))


dynamodb_client = boto3.resource("dynamodb")
market_event_publisher: MarketEventPublisher | None = None


def get_active_candidates(table_name: str) -> set[str]:
	"""Return unexpired symbols from the table for the configured universe."""
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
	transport: str | MarketEventTransport,
	*,
	stream_name: str | None = None,
	topic_arn: str | None = None,
) -> MarketEventPublisher:
	"""Create the publisher configured for this worker process."""
	try:
		normalized_transport = MarketEventTransport(transport.strip().lower())
	except ValueError as error:
		raise ValueError(f"Unsupported MARKET_EVENT_TRANSPORT: {transport}") from error

	match normalized_transport:
		case MarketEventTransport.LOG:
			return LoggingMarketEventPublisher(logger)
		case MarketEventTransport.KINESIS:
			if not stream_name:
				raise RuntimeError("MARKET_EVENT_STREAM_NAME is required for kinesis")
			return KinesisMarketEventPublisher(stream_name)
		case MarketEventTransport.SNS:
			if not topic_arn:
				raise RuntimeError("MARKET_EVENT_TOPIC_ARN is required for sns")
			return SnsMarketEventPublisher(topic_arn)
	raise AssertionError(f"Unhandled market event transport: {normalized_transport}")


def handle_market_data_message(
	message: dict,
	publisher: MarketEventPublisher | None = None,
) -> None:
	"""Convert a subscribed symbol's yfinance message to a market quote event.

	Publish the event to Kinesis, SNS, or the log using the selected publisher.
	If no publisher is passed, use the worker's configured publisher.
	Publishing errors are logged; conversion and configuration errors propagate.
	"""
	## TODO: abstract the quote_event_from_yfinance_message function to a provider module,
	##  so that this function can be more generic and not tied to yfinance specifically.
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


async def run_websocket_session() -> None:
	"""Listen for quotes while refreshing subscriptions to active candidates.

	Listening and subscription updates run concurrently, so subscription changes
	may produce duplicate messages. Downstream processing must handle duplicates.
	"""
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

				active_symbols = set(
					get_active_candidates(ACTIVE_CANDIDATES_TABLE_NAME)
				)

				symbols_to_add = active_symbols - subscribed_symbols
				symbols_to_remove = subscribed_symbols - active_symbols

				if symbols_to_add:
					logger.info("Subscribing to symbols: %s", sorted(symbols_to_add))
					await ws.subscribe(sorted(symbols_to_add))

				if symbols_to_remove:
					logger.info(
						"Unsubscribing from symbols: %s", sorted(symbols_to_remove)
					)
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
	"""Run WebSocket sessions continuously, waiting longer after each failure."""
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
	"""Check the worker settings, create its publisher, and start collecting quotes."""
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
