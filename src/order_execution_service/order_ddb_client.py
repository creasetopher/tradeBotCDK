import boto3
from datetime import datetime, timezone
from typing import Any
from tradebot.brokers import OrderResult

class OrderDynamoDbClient:
    def __init__(
        self,
        table_name: str,
        *,
        dynamodb_resource: Any | None = None,
    ) -> None:
        dynamodb = dynamodb_resource or boto3.resource("dynamodb")
        self.table = dynamodb.Table(table_name)

    def save_order_result(self, order_result: OrderResult) -> None:
        self.table.put_item(
            Item={
                "client_order_id": order_result.client_order_id,
                "broker_order_id": order_result.broker_order_id,
                "symbol": order_result.symbol,
                "status": order_result.status,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )
