#!/usr/bin/env python3
import os

import aws_cdk as cdk

from trade_bot_cdk.config import resolve_market_event_transport
from trade_bot_cdk.trade_bot_cdk_stack import TradeBotCdkStack


app = cdk.App()

# fallback on dev if no stage is provided via context or environment variable
stage = app.node.try_get_context("stage") or os.getenv("STAGE", "dev")
market_event_transport = resolve_market_event_transport(
    stage,
    app.node.try_get_context("marketEventTransport"),
)


TradeBotCdkStack(
    app,
    f"TradeBotCdkStack-{stage}",
    stage=stage,
    market_event_transport=market_event_transport,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
