from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from pydantic import BaseModel

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
candidate_history_table = dynamodb.Table(os.environ["CANDIDATES_TABLE_NAME"])
active_candidates_table = dynamodb.Table(os.environ["ACTIVE_CANDIDATES_TABLE_NAME"])

UNIVERSE_ID = os.environ.get("UNIVERSE_ID", "default_universe")
CANDIDATE_TTL_DAYS = int(os.environ.get("CANDIDATE_TTL_DAYS", "14"))
ACTIVE_CANDIDATE_TTL_SECONDS = int(os.environ.get("ACTIVE_CANDIDATE_TTL_SECONDS", "900"))

# triggered by SNS topic, writes candidate records to DynamoDB with TTL for later processing by market data worker
def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    failures: list[dict[str, str]] = []

    for record in event.get("Records", []):
        try:
            payload = _extract_payload(record)
            candidate_event = CandidateSnapshotEvent.model_validate(payload)

            candidate_history_item = _candidate_history_item(candidate_event)
            active_candidate_item = _active_candidate_item(candidate_event)

            candidate_history_table.put_item(Item=candidate_history_item)
            active_candidates_table.put_item(Item=active_candidate_item)

        except Exception:
            logger.exception("Failed to process candidate record")
            failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": failures}


def _extract_payload(record: dict[str, Any]) -> dict[str, Any]:
    body = json.loads(record["body"])
    # Supports both raw SNS delivery and the default SNS envelope shape.
    # CDK uses raw_message_delivery=True, so body is usually the payload.
    if isinstance(body, dict) and "Message" in body:
        return json.loads(body["Message"])
    return body

def _candidate_history_item(event: CandidateSnapshotEvent) -> dict[str, Any]:
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


# helper function to recursively convert values to DynamoDB-compatible formats
def _to_dynamodb_item(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _to_dynamodb_value(value.model_dump(mode="python", exclude_none=True))

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, Decimal):
        return value

    if isinstance(value, float):
        # Last-resort guard. Ideally floats should never reach this point.
        return Decimal(str(value))

    if isinstance(value, dict):
        return {
            str(k): _to_dynamodb_value(v)
            for k, v in value.items()
            if v is not None
        }

    if isinstance(value, list):
        return [_to_dynamodb_value(v) for v in value]

    return value