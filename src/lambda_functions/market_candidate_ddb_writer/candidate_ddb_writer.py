"""Persist scanner candidate events in DynamoDB."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel
from tradebot.events.candidate import CandidateSnapshotEvent

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
candidate_history_table = dynamodb.Table(os.environ["CANDIDATES_TABLE_NAME"])
active_candidates_table = dynamodb.Table(os.environ["ACTIVE_CANDIDATES_TABLE_NAME"])

UNIVERSE_ID = os.environ.get("UNIVERSE_ID", "default_universe")
CANDIDATE_TTL_DAYS = int(os.environ.get("CANDIDATE_TTL_DAYS", "14"))
ACTIVE_CANDIDATE_TTL_SECONDS = int(
	os.environ.get("ACTIVE_CANDIDATE_TTL_SECONDS", "900")
)


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
	"""Write candidate events from SQS to the history and active DynamoDB tables.

	Messages arrive through the candidate SNS topic. Records include expiration
	times so the market data worker can select active candidates.
	Return failed message IDs so SQS can retry them.
	"""
	logger.info(f"Received incoming event: {json.dumps(event)}")
	failures: list[dict[str, str]] = []

	for record in event.get("Records", []):
		try:
			payload = _extract_payload(record)
			candidate_event = CandidateSnapshotEvent.model_validate(payload)

			candidate_history_item = _candidate_history_item(candidate_event)
			active_candidate_item = _active_candidate_item(candidate_event)

			candidate_history_table.put_item(Item=candidate_history_item)
			_conditional_put_active_candidate_item(active_candidate_item)

			# leaving this here for reference, but the conditional put function handles this logic now
			# active_candidates_table.put_item(Item=active_candidate_item)

		except Exception:
			logger.exception("Failed to process candidate record")
			failures.append({"itemIdentifier": record["messageId"]})

	return {"batchItemFailures": failures}


def _extract_payload(record: dict[str, Any]) -> dict[str, Any]:
	"""Read candidate JSON from an SQS body, unwrapping an SNS envelope if present."""
	body = json.loads(record["body"])
	# Supports both raw SNS delivery and the default SNS envelope shape.
	# CDK uses raw_message_delivery=True, so body is usually the payload.
	if isinstance(body, dict) and "Message" in body:
		return json.loads(body["Message"])
	return body


def _candidate_history_item(event: CandidateSnapshotEvent) -> dict[str, Any]:
	"""Build a candidate history record with its configured expiration time."""
	candidate = event.candidate
	update_time = candidate.update_time.astimezone(timezone.utc).isoformat()

	item = {
		"symbol": candidate.symbol,
		"update_time": update_time,
		"event_type": event.event_type,
		"schema_version": event.schema_version,
		"event_id": event.event_id,
		"source": event.source,
		"scanner_tags": candidate.scanner_tags,
		"price": candidate.price,
		"volume": candidate.volume,
		"dollar_volume": candidate.dollar_volume,
		"spread": candidate.spread,
		"spread_bps": candidate.spread_bps,
		"change_percent": candidate.change_percent,
		"candidate": candidate,
		"event": event,
		"expires_at": int(time.time()) + CANDIDATE_TTL_DAYS * 24 * 60 * 60,
	}

	return _to_dynamodb_item(item)


def _active_candidate_item(event: CandidateSnapshotEvent) -> dict[str, Any]:
	"""Build an active candidate record for the worker, including its expiration time."""
	candidate = event.candidate
	update_time = candidate.update_time.astimezone(timezone.utc).isoformat()

	item = {
		"universe_id": UNIVERSE_ID,
		"symbol": candidate.symbol,
		"last_seen_update_time": update_time,
		"event_id": event.event_id,
		"source": event.source,
		"scanner_tags": candidate.scanner_tags,
		"price": candidate.price,
		"volume": candidate.volume,
		"dollar_volume": candidate.dollar_volume,
		"spread": candidate.spread,
		"spread_bps": candidate.spread_bps,
		"change_percent": candidate.change_percent,
		"candidate": candidate,
		"expires_at": int(time.time()) + ACTIVE_CANDIDATE_TTL_SECONDS,
	}

	return _to_dynamodb_item(item)


def _conditional_put_active_candidate_item(item: dict[str, Any]) -> None:
	"""Save an active candidate unless a newer update is already stored."""
	condition_expression = "attribute_not_exists(last_seen_update_time) OR last_seen_update_time <= :update_time"
	CONDITIONAL_EXP_FAILED_RESPONSE = "ConditionalCheckFailedException"
	try:
		active_candidates_table.put_item(
			Item=item,
			ConditionExpression=condition_expression,
			ExpressionAttributeValues={
				":update_time": item["last_seen_update_time"],
			},
		)
	except ClientError as exc:
		if exc.response["Error"]["Code"] == CONDITIONAL_EXP_FAILED_RESPONSE:
			logger.info(
				"Skipping stale active candidate update for symbol=%s update_time=%s",
				item["symbol"],
				item["last_seen_update_time"],
			)
			return

		raise


def _to_dynamodb_item(value: Any) -> Any:
	"""Recursively convert models and values into DynamoDB-compatible data.

	Convert dates to UTC strings and floats to Decimal, and omit dictionary
	entries whose values are None.
	"""
	if isinstance(value, BaseModel):
		return _to_dynamodb_item(value.model_dump(mode="python", exclude_none=True))

	if isinstance(value, datetime):
		return value.astimezone(timezone.utc).isoformat()

	if isinstance(value, Decimal):
		return value

	if isinstance(value, float):
		# Last-resort guard. Ideally floats should never reach this point.
		return Decimal(str(value))

	if isinstance(value, dict):
		return {str(k): _to_dynamodb_item(v) for k, v in value.items() if v is not None}

	if isinstance(value, list):
		return [_to_dynamodb_item(v) for v in value]

	return value
