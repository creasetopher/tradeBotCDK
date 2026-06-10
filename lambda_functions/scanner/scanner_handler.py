from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from defined_screener_queries import YF_PREDEFINED_SCREENER_QUERIES
import boto3
import yfinance as yf
from tradebot.events.candidate import CandidateSnapshotEvent

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")

# CANDIDATE_TOPIC_ARN = os.environ["CANDIDATE_TOPIC_ARN"]
CANDIDATE_TOPIC_ARN = "CANDIDATE_TOPIC_ARN"
DEFAULT_SCREENS = [
    YF_PREDEFINED_SCREENER_QUERIES.SMALL_CAP_GAINERS, 
    YF_PREDEFINED_SCREENER_QUERIES.DAY_GAINERS, 
    YF_PREDEFINED_SCREENER_QUERIES.AGGRESSIVE_SMALL_CAPS
]
SCREENS = [s.strip() for s in os.environ.get("SCREENS", "").split(",") if s.strip()] if os.environ.get("SCREENS", None) else DEFAULT_SCREENS
MAX_SYMBOLS_PER_SCREEN = int(os.environ.get("MAX_SYMBOLS_PER_SCREEN", "100"))
MIN_PRICE = Decimal(os.environ.get("MIN_PRICE", "1.00"))
MIN_DOLLAR_VOLUME = Decimal(os.environ.get("MIN_DOLLAR_VOLUME", "1000000"))
EVENT_TYPE = "candidate.snapshot.v1"
SCHEMA_VERSION = "1.0.0"
SOURCE = "yfinance(YAHOO_FINANCE)_predefined_screens"

def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    update_time = datetime.now(timezone.utc).isoformat()
    candidates_by_symbol: dict[str, dict[str, Any]] = {}

    for screen in SCREENS:
        logger.info("Running yfinance screen: %s", screen)

        try:
            response = yf.screen(screen, count=MAX_SYMBOLS_PER_SCREEN)
        except Exception:
            logger.exception("Screen failed: %s", screen)
            continue

        for quote in response.get("quotes", []):
            candidate = _candidate_from_quote(
                quote=quote, 
                screen=screen, 
                as_of=update_time
            )
            if not _passes_liquidity_filters(candidate):
                continue

            symbol = candidate["symbol"]
            if symbol not in candidates_by_symbol:
                candidates_by_symbol[symbol] = candidate
            else:
                # A symbol can appear in multiple screens. Keep one candidate record with tags.
                existing_tags = set(candidates_by_symbol[symbol].get("scanner_tags", []))
                existing_tags.update(candidate.get("scanner_tags", []))
                candidates_by_symbol[symbol]["scanner_tags"] = sorted(existing_tags)

    published = 0
    for candidate in candidates_by_symbol.values():
        event = CandidateSnapshotEvent.from_candidate(candidate)

        # payload = {
        #     "event_type": EVENT_TYPE,
        #     "schema_version": SCHEMA_VERSION,
        #     "event_id": _build_candidate_event_id(candidate),
        #     "source": SOURCE,
        #     "update_time": update_time,
        #     "candidate": candidate,
        # }
        sns_client.publish(
            TopicArn=CANDIDATE_TOPIC_ARN,
            Message=json.dumps(payload, separators=(",", ":")),
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": event.event_type,
                },
                "symbol": {
                    "DataType": "String",
                    "StringValue": event.candidate.symbol,
                },
                "schema_version": {
                    "DataType": "String",
                    "StringValue": event.schema_version,
                },
                "event_id": {
                    "DataType": "String",
                    "StringValue": event.event_id,
                },

            },
        )
        published += 1

    body = {
        "screen_count": len(SCREENS),
        "candidate_count": len(candidates_by_symbol),
        "published": published,
        "update_time": update_time,
    }
    logger.info("Scanner complete: %s", body)
    
    return {"statusCode": 200, "body": json.dumps(body)}


def _candidate_from_quote(quote: dict[str, Any], screen: str, as_of: str) -> dict[str, Any] | None:
    symbol = quote.get("symbol")
    if not symbol:
        return None

    price = _as_decimal(quote.get("regularMarketPrice"))
    volume = _as_decimal(quote.get("regularMarketVolume"))
    previous_close = _as_decimal(quote.get("regularMarketPreviousClose"))
    dollar_volume = price * volume if price is not None and volume is not None else Decimal("0")

    return {
        "symbol": str(symbol).upper(),
        "update_time": as_of,
        "scanner_tags": [screen],
        "price": str(price) if price is not None else None,
        "volume": str(volume) if volume is not None else None,
        "dollar_volume": str(dollar_volume),
        "previous_close": str(previous_close) if previous_close is not None else None,
        "day_high": _string_decimal(quote.get("regularMarketDayHigh")),
        "day_low": _string_decimal(quote.get("regularMarketDayLow")),
        "open": _string_decimal(quote.get("regularMarketOpen")),
        "bid": _string_decimal(quote.get("bid")),
        "ask": _string_decimal(quote.get("ask")),
        "short_name": quote.get("shortName"),
        "analyst_rating": quote.get("averageAnalystRating"),
        "source_payload_ts_ms": int(time.time() * 1000),
    }


def _passes_liquidity_filters(candidate: dict[str, Any]) -> bool:
    if not candidate:
        return False
    price = _as_decimal(candidate.get("price"))
    dollar_volume = _as_decimal(candidate.get("dollar_volume"))
    if price is None or dollar_volume is None:
        return False
    return price >= MIN_PRICE and dollar_volume >= MIN_DOLLAR_VOLUME

def _build_candidate_event_id(candidate: dict[str, Any]) -> str:
    """
    Build a deterministic event ID based on candidate attributes for idempotency and deduplication purposes.
    """
    scanner_tags = ",".join(sorted(candidate.get("scanner_tags", [])))

    raw = "|".join(
        [
            EVENT_TYPE,
            SOURCE,
            candidate["symbol"].upper(),
            candidate["update_time"],
            scanner_tags,
        ]
    )

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"evt_{digest}"

def _string_decimal(value: Any) -> str | None:
    value_as_decimal = _as_decimal(value)
    return str(value_as_decimal) if value_as_decimal is not None else None


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        logger.warning("Failed to convert value to Decimal: %s", value)
        return None