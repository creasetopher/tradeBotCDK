"""Write normalized market events to S3 and DynamoDB."""

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

MARKET_EVENT_TTL_DAYS = int(os.environ.get("MARKET_EVENT_TTL_DAYS", "7"))
SUPPORTED_EVENT_SOURCES = {"aws:kinesis", "aws:sqs"}

s3_client: Any | None = None
table: Any | None = None


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
	"""Write raw Kinesis or SQS events to S3 and their metadata to DynamoDB.

	Return a partial-batch response identifying failed records for retry.
	"""
	failures: list[dict[str, str]] = []
	parsed_records: list[tuple[str, dict[str, Any]]] = []

	for record in event.get("Records", []):
		record_identifier = RecordDecoder.get_record_identifier(record)
		try:
			parsed_record = RecordDecoder.decode_record(record)
			parsed_records.append(parsed_record)
		except Exception:
			logger.exception("Failed to decode market event record")
			failures.append({"itemIdentifier": record_identifier})

	if len(parsed_records) == 0:
		return {"batchItemFailures": failures}

	bucket_name = os.environ["MARKET_DATA_BUCKET_NAME"]
	resolved_s3_client = _get_s3_client()
	resolved_table = _get_table()
	now = datetime.now(timezone.utc)
	s3_key = (
		f"raw/source=market-data/date={now:%Y-%m-%d}/hour={now:%H}/"
		f"batch-{context.aws_request_id}.jsonl"
	)
	body = _parsed_records_to_s3_body(parsed_records)

	try:
		resolved_s3_client.put_object(
			Bucket=bucket_name,
			Key=s3_key,
			Body=body.encode("utf-8"),
			ContentType="application/x-ndjson",
		)
		s3_uri = f"s3://{bucket_name}/{s3_key}"
		expires_at = int(time.time()) + MARKET_EVENT_TTL_DAYS * 24 * 60 * 60

		with resolved_table.batch_writer() as batch:
			for _, body in parsed_records:
				symbol = str(body.get("symbol") or body.get("id") or "UNKNOWN").upper()
				event_ts_ms = _event_ts_ms_from_payload(body)
				day = datetime.fromtimestamp(
					event_ts_ms / 1000, tz=timezone.utc
				).strftime("%Y-%m-%d")
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
				batch.put_item(Item=_to_dynamodb_item(item))
	except Exception:
		logger.exception("Failed to persist market event batch")
		failures.extend(
			{"itemIdentifier": record_identifier}
			for record_identifier, _ in parsed_records
		)

	return {"batchItemFailures": failures}


def _get_s3_client() -> Any:
	"""Create the S3 client on first use and reuse it across invocations."""
	global s3_client
	if s3_client is None:
		s3_client = boto3.client("s3")
	return s3_client


def _get_table() -> Any:
	"""Create the DynamoDB table resource on first use and reuse it."""
	global table
	if table is None:
		table_name = os.environ["MARKET_EVENTS_TABLE_NAME"]
		table = boto3.resource("dynamodb").Table(table_name)
	return table


def _event_ts_ms_from_payload(body: dict[str, Any]) -> int:
	"""Return the event timestamp in milliseconds, falling back to the current time.

	Prefer event_ts_ms, then the quote's source_payload_ts_ms. If neither is
	present, parse event_time from the event or its nested quote. These event
	times are ISO 8601 strings rather than millisecond timestamps.
	"""
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
	"""Copy JSON-compatible data, converting integers and floats to Decimal."""
	return json.loads(json.dumps(value), parse_float=Decimal, parse_int=Decimal)


def _parsed_records_to_s3_body(parsed_records: list[tuple[str, dict[str, Any]]]) -> str:
	"""Return one compact JSON payload per line, with a final newline, for S3."""
	return (
		"\n".join(
			json.dumps(payload, separators=(",", ":")) for _, payload in parsed_records
		)
		+ "\n"
	)


class RecordDecoder:
	"""Decode Kinesis and SQS records separately from the handler for easier testing."""
	@classmethod
	def _decode_kinesis_payload(
		cls, encoded_record: dict[str, Any]
	) -> tuple[str, dict[str, Any]]:
		"""Return the sequence number and decoded JSON from a base64 Kinesis record."""
		sequence_number = encoded_record["kinesis"]["sequenceNumber"]
		return sequence_number, json.loads(
			base64.b64decode(encoded_record["kinesis"]["data"]).decode("utf-8")
		)

	@classmethod
	def _decode_sqs_payload(cls, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
		"""Decode an SQS record whose body contains raw SNS-delivered event JSON."""
		return record["messageId"], json.loads(record["body"])

	@classmethod
	def decode_record(cls, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
		"""Choose the Kinesis or SQS decoder and return the record ID and payload."""
		record_decoder_mapper = {
			"aws:kinesis": cls._decode_kinesis_payload,
			"aws:sqs": cls._decode_sqs_payload,
		}
		event_source = cls._get_event_source(record)
		if event_source in record_decoder_mapper:
			return record_decoder_mapper[event_source](record)
		raise ValueError(
			f"There's no mapping to a decoder for event source: {event_source}"
		)

	@classmethod
	def _get_event_source(cls, record: dict[str, Any]) -> str:
		"""Return a case-normalized Lambda record event source."""
		event_source = record.get("eventSource")
		if not isinstance(event_source, str) or not event_source.strip():
			raise ValueError("Market event record has no eventSource")
		normalized_event_source = event_source.strip().lower()
		if normalized_event_source not in SUPPORTED_EVENT_SOURCES:
			raise ValueError(
				f"Unsupported market event source: {normalized_event_source}"
			)
		return normalized_event_source

	@classmethod
	def get_record_identifier(cls, record: dict[str, Any]) -> str:
		"""Return the record ID Lambda expects in a partial-batch failure."""
		record_identifier_mapper = {
			"aws:kinesis": lambda r: str(r["kinesis"]["sequenceNumber"]),
			"aws:sqs": lambda r: str(r["messageId"]),
		}
		event_source = cls._get_event_source(record)
		if event_source in record_identifier_mapper:
			return record_identifier_mapper[event_source](record)
		raise ValueError(f"Unidentified event source: {event_source}")
