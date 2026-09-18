import json
import os
from decimal import Decimal
from typing import Any

from tradebot.brokers import (
    OrderRequest,
    OrderSide,
    OrderType,
    TimeInForce,
)

from .broker_factory import BrokerMode, create_order_broker
from .service import OrderExecutionService
import logging

logger = logging.getLogger(__name__)

def _create_order_execution_service() -> OrderExecutionService:
    broker = create_order_broker(
        BrokerMode(os.getenv("ORDER_BROKER_MODE", BrokerMode.LOG.value)),
        alpaca_api_key=os.getenv("ALPACA_API_KEY"),
        alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY"),
    )

    return OrderExecutionService(broker)


def _order_from_sqs_message(body: str) -> OrderRequest:
    message: dict[str, Any] = json.loads(body)

    return OrderRequest(
        client_order_id=message["client_order_id"],
        symbol=message["symbol"],
        quantity=Decimal(str(message["quantity"])),
        side=OrderSide(message["side"]),
        order_type=OrderType(message["order_type"]),
        time_in_force=TimeInForce(
            message.get("time_in_force", TimeInForce.DAY.value)
        ),
        limit_price=(
            Decimal(str(message["limit_price"]))
            if message.get("limit_price") is not None
            else None
        ),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    service = _create_order_execution_service()
    failures: list[dict[str, str]] = []

    for record in event["Records"]:
        try:
            order = _order_from_sqs_message(record["body"])
            service.execute(order)
        except Exception:
            logger.exception(
                "Failed to process order message message_id=%s \n\nrecord -> %s",
                record.get("messageId"),
                str(record),
            )
            failures.append(
                {"itemIdentifier": record["messageId"]}
            )

    return {"batchItemFailures": failures}
