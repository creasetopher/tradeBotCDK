"""Live end-to-end test for the market-data ingestion pipeline.

The test covers:

ActiveCandidatesTable -> Fargate worker -> yfinance WebSocket ->
market.quote.v1 -> Kinesis -> MarketEventWriter -> S3 JSONL + MarketEventsTable

It intentionally uses the public yfinance feed, so run it while US equities are
actively publishing quotes. Set RUN_E2E=1 to opt in.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pytest
from boto3.dynamodb.conditions import Key


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("RUN_E2E") != "1",
        reason="set RUN_E2E=1 to run tests against a deployed AWS stack",
    ),
]


POLL_INTERVAL_SECONDS = 10
DEFAULT_TIMEOUT_SECONDS = 12 * 60


def _stack_outputs(cloudformation: Any, stack_name: str) -> dict[str, str]:
    """Return a deployed CloudFormation stack's outputs as a name/value mapping.

    CloudFormation represents outputs as a list of objects containing
    ``OutputKey`` and ``OutputValue`` fields. Converting that list to a
    dictionary lets the test look up resources by names such as
    ``MarketDataBucketName`` without hard-coding their generated AWS names.

    Args:
        cloudformation: A boto3 CloudFormation client.
        stack_name: The name or unique ID of the deployed stack.

    Returns:
        A dictionary mapping each CloudFormation output key to its string value.

    Raises:
        botocore.exceptions.ClientError: If the stack does not exist or AWS
            rejects the request.
    """
    response = cloudformation.describe_stacks(StackName=stack_name)
    outputs = response["Stacks"][0].get("Outputs", [])
    return {output["OutputKey"]: output["OutputValue"] for output in outputs}


def _wait_until(description: str, timeout_seconds: int, probe: Any) -> Any:
    """Poll a probe until it returns a non-None result or time runs out.

    The pipeline is asynchronous, so a quote will not appear in S3 or DynamoDB
    immediately. The probe is called every ``POLL_INTERVAL_SECONDS``. A
    ``None`` result means the expected state is not visible yet. Exceptions are
    treated as potentially transient and remembered for the timeout message.

    Args:
        description: Human-readable description of the state being awaited.
        timeout_seconds: Maximum number of seconds to keep polling.
        probe: A zero-argument callable that returns the desired result when it
            is available, or ``None`` when the test should continue polling.

    Returns:
        The first non-None value returned by ``probe``.

    Raises:
        pytest.fail.Exception: If the expected result does not appear before
            the timeout. The failure includes the most recent probe exception.
    """
    # use monotonic() to avoid issues with system clock adjustments
    # this can be a problem in CI/CD pipelines that sync the clock with NTP servers, 
    # (an NTP server is a server that provides the current time to clients on a network)
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            result = probe()
            if result is not None:
                return result
        except Exception as error:  # transient AWS/eventual-consistency failures
            last_error = error
        time.sleep(POLL_INTERVAL_SECONDS)

    detail = f" Last probe error: {last_error!r}" if last_error else ""
    pytest.fail(f"Timed out waiting for {description}.{detail}")


def _find_quote_in_new_jsonl_objects(
    s3: Any,
    bucket_name: str,
    symbol: str,
    test_started_at: datetime,
) -> tuple[dict[str, Any], str] | None:
    """Find this test's symbol in recently written market-data JSONL objects.

    The function lists objects under the raw market-data S3 prefix, ignores
    objects older than the test, and searches the newest objects first. Each
    non-empty line is decoded as one JSON event. A match must have both the
    requested symbol and the ``market.quote.v1`` event type.

    Args:
        s3: A boto3 S3 client.
        bucket_name: Name of the market-data bucket to search.
        symbol: Uppercase ticker symbol expected in the quote event.
        test_started_at: Earliest S3 object modification time to consider.

    Returns:
        A tuple containing the decoded quote payload and its full ``s3://`` URI,
        or ``None`` when no matching quote is currently available.

    Raises:
        botocore.exceptions.ClientError: If AWS rejects an S3 operation.
        json.JSONDecodeError: If a line in a candidate object is not valid JSON.
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix="raw/source=market-data/")

    objects = [
        item
        for page in pages
        for item in page.get("Contents", [])
        if item["LastModified"] >= test_started_at
    ]
    for item in sorted(objects, key=lambda value: value["LastModified"], reverse=True):
        response = s3.get_object(Bucket=bucket_name, Key=item["Key"])
        for line in response["Body"].read().decode("utf-8").splitlines():
            payload = json.loads(line)
            if (
                str(payload.get("symbol", "")).upper() == symbol
                and payload.get("event_type") == "market.quote.v1"
            ):
                return payload, f"s3://{bucket_name}/{item['Key']}"
    return None


def _event_timestamp_ms(payload: dict[str, Any]) -> int:
    quote = payload.get("quote") or {}
    if quote.get("source_payload_ts_ms") is not None:
        return int(quote["source_payload_ts_ms"])

    event_time = payload.get("event_time") or quote.get("event_time")
    if not event_time:
        raise AssertionError("market.quote.v1 payload has no event timestamp")
    parsed = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def test_active_candidate_reaches_s3_and_market_events_table() -> None:
    """Prove a real yfinance quote traverses every deployed pipeline stage."""
    stage = os.environ.get("E2E_STAGE", "dev")
    stack_name = os.environ.get("E2E_STACK_NAME", f"TradeBotCdkStack-{stage}")
    symbol = os.environ.get("E2E_SYMBOL", "AAPL").strip().upper()
    universe_id = os.environ.get("E2E_UNIVERSE_ID", "default")
    timeout_seconds = int(os.environ.get("E2E_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    session = boto3.Session(region_name=os.environ.get("AWS_REGION"))

    cloudformation = session.client("cloudformation")
    ecs = session.client("ecs")
    s3 = session.client("s3")
    dynamodb = session.resource("dynamodb")
    outputs = _stack_outputs(cloudformation, stack_name)

    required_outputs = {
        "ActiveCandidatesTableName",
        "MarketDataBucketName",
        "MarketEventsTableName",
        "MarketDataWorkerClusterName",
        "MarketDataWorkerServiceName",
    }
    missing = required_outputs - outputs.keys()
    # check for missing outputs, ensuring the stack is deployed and outputs are available
    assert not missing, f"Deploy the current CDK stack; missing outputs: {sorted(missing)}"

    active_table = dynamodb.Table(outputs["ActiveCandidatesTableName"])
    events_table = dynamodb.Table(outputs["MarketEventsTableName"])
    # check that the worker cluster and service exist, ensuring the pipeline is deployed
    cluster = outputs["MarketDataWorkerClusterName"]

    # check that the worker service is running, ensuring the pipeline is active
    service = outputs["MarketDataWorkerServiceName"]
    service_state = ecs.describe_services(cluster=cluster, services=[service])["services"][0]
    original_desired_count = service_state["desiredCount"]
    # S3 LastModified has coarser precision than datetime.now(). The small
    # overlap avoids missing an object written in the same wall-clock second.
    test_started_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    active_candidate_key = {"universe_id": universe_id, "symbol": symbol}
    previous_candidate = active_table.get_item(
        Key=active_candidate_key,
        ConsistentRead=True,
    ).get("Item")

    active_table.put_item(
        Item={
            **active_candidate_key,
            "expires_at": int(time.time()) + timeout_seconds + 300,
            "source": "pytest-market-data-e2e",
        }
    )

    try:
        if original_desired_count < 1:
            ecs.update_service(cluster=cluster, service=service, desiredCount=1)

        payload, s3_uri = _wait_until(
            f"a {symbol} market.quote.v1 JSONL record in S3",
            timeout_seconds,
            lambda: _find_quote_in_new_jsonl_objects(
                s3,
                outputs["MarketDataBucketName"],
                symbol,
                test_started_at,
            ),
        )

        event_ts_ms = _event_timestamp_ms(payload)
        event_day = datetime.fromtimestamp(
            event_ts_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")

        def find_correlated_dynamodb_item() -> dict[str, Any] | None:
            response = events_table.query(
                KeyConditionExpression=(
                    Key("symbol_day").eq(f"{symbol}#{event_day}")
                    & Key("event_ts_ms").eq(event_ts_ms)
                ),
                ConsistentRead=True,
            )
            return next(
                (
                    item
                    for item in response.get("Items", [])
                    if item.get("event_type") == "market.quote.v1"
                    and item.get("s3_uri") == s3_uri
                ),
                None,
            )

        item = _wait_until(
            "the correlated MarketEventsTable row",
            timeout_seconds,
            find_correlated_dynamodb_item,
        )
        assert item["symbol"] == symbol
        assert item["source"] == "yfinance.websocket"
        assert payload["schema_version"] == "1.0.0"
    finally:
        if previous_candidate is None:
            active_table.delete_item(Key=active_candidate_key)
        else:
            active_table.put_item(Item=previous_candidate)
        if original_desired_count < 1:
            ecs.update_service(
                cluster=cluster,
                service=service,
                desiredCount=original_desired_count,
            )
