from dataclasses import dataclass
from enum import Enum


class OrderFailureCode(str, Enum):
    EXPIRED = "expired"
    INVALID_QUANTITY = "invalid_quantity"
    QUANTITY_LIMIT_EXCEEDED = "quantity_limit_exceeded"
    TRADING_DISABLED = "trading_disabled"
    KILL_SWITCH_ACTIVE = "kill_switch_active"
    DUPLICATE_ORDER = "duplicate_order"
    BROKER_REJECTED = "broker_rejected"
    UNCERTAIN_SUBMISSION = "uncertain_submission"


class OrderFailureStage(str, Enum):
    VALIDATION = "validation"
    SAFETY_CHECK = "safety_check"
    BROKER_SUBMISSION = "broker_submission"
    PERSISTENCE = "persistence"


@dataclass(frozen=True)
class OrderFailure:
    code: OrderFailureCode
    stage: OrderFailureStage
    reason: str

class ExpectedOrderFailure(Exception):
    def __init__(self, failure: OrderFailure) -> None:
        super().__init__(failure.reason)
        self.failure = failure
