#!/usr/bin/env python3
import os

import aws_cdk as cdk

from trade_bot_cdk.trade_bot_cdk_stack import TradeBotCdkStack


app = cdk.App()
stage = app.node.try_get_context("stage") or os.getenv("STAGE", "dev")


TradeBotCdkStack(
    app,
    f"TradeBotCdkStack-{stage}",
    stage=stage,
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region=os.getenv("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
