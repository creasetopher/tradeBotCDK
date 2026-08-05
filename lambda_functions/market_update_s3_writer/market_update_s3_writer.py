from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = os.environ["MARKET_DATA_BUCKET_NAME"]
TABLE_NAME = os.environ["MARKET_EVENTS_TABLE_NAME"]
MARKET_EVENT_TTL_DAYS = int(os.environ.get("MARKET_EVENT_TTL_DAYS", "7"))

s3_client = boto3.client("s3")
dynamodb_client = boto3.resource("dynamodb")

table = dynamodb_client.Table(TABLE_NAME)

# triggered by kineses stream, writes raw events to S3 and metadata to DynamoDB for later processing by candidate writer
def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []
    parsed_records: list[tuple[str, dict[str, Any]]] = []

    for record in event.get("Records", []):
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            parsed_record: tuple[str, dict[str, Any]] = _decode_kinesis_payload(record)
            parsed_records.append(parsed_record)
        except Exception:
            logger.exception("Failed to decode Kinesis record")
            failures.append({"itemIdentifier": sequence_number})

    if len(parsed_records) == 0:
        return {"batchItemFailures": failures}

    now = datetime.now(timezone.utc)
    s3_key = (
        f"raw/source=market-data/date={now:%Y-%m-%d}/hour={now:%H}/"
        f"batch-{context.aws_request_id}.jsonl"
    )
    body = _parsed_records_to_s3_body(parsed_records)

    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
        expires_at = int(time.time()) + MARKET_EVENT_TTL_DAYS * 24 * 60 * 60

        with table.batch_writer() as batch:
            for _, body in parsed_records:
                symbol = str(body.get("symbol") or body.get("id") or "UNKNOWN").upper()
                event_ts_ms = _event_ts_ms_from_payload(body)
                day = datetime.fromtimestamp(event_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                item = {
                    "symbol_day": f"{symbol}#{day}",
                    "event_ts_ms": event_ts_ms,
                    "symbol": symbol,
                    "event_type": body.get("event_type", "quote"),
                    "schema_version": body.get("schema_version", "1.0"),
                    "source": body.get("source", "unknown"),
                    "s3_uri": s3_uri,
                    "expires_at": expires_at,
                }
                batch.put_item(Item = _to_dynamodb_item(item))
    except Exception:
        logger.exception("Failed to persist Kinesis batch")
        failures.extend({"itemIdentifier": sequence_number} for sequence_number, _ in parsed_records)

    return {"batchItemFailures": failures}

# parses the event body to determine the event timestamp in milliseconds, using a series of fallbacks if the expected fields are not present
# we do this becasue the market event stores event_time as a string in ISO 8601 format, 
# but the quote payloads may have a source_payload_ts_ms field that is an integer timestamp in milliseconds, 
# and we want to use that if it is present
def _event_ts_ms_from_payload(body: dict[str, Any]) -> int:
    if body.get("event_ts_ms") is not None:
        return int(body["event_ts_ms"])

    quote = body.get("quote") or {}

    if quote.get("source_payload_ts_ms") is not None:
        return int(quote["source_payload_ts_ms"])

    if body.get("event_time") is not None:
        event_time = body["event_time"]
        dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)

    if quote.get("event_time") is not None:
        dt = datetime.fromisoformat(quote["event_time"].replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)

    return int(time.time() * 1000)

def _to_dynamodb_item(value: Any) -> Any:
    """
    Convert floats and ints to Decimals for DynamoDB compatibility, and ensure all keys and values are strings where possible.
    """
    return json.loads(json.dumps(value), parse_float=Decimal, parse_int=Decimal)

def _decode_kinesis_payload(encoded_record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sequence_number = encoded_record["kinesis"]["sequenceNumber"]
    return sequence_number, json.loads(base64.b64decode(encoded_record["kinesis"]["data"]).decode("utf-8"))

def _parsed_records_to_s3_body(parsed_records: list[tuple[str, dict[str, Any]]]) -> str:
    return "\n".join(json.dumps(payload, separators=(",", ":")) for _, payload in parsed_records) + "\n"