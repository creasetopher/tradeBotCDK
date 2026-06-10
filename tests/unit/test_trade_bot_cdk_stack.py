import aws_cdk as core
import aws_cdk.assertions as assertions

from trade_bot_cdk.trade_bot_cdk_stack import TradeBotCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in trade_bot_cdk/trade_bot_cdk_stack.py
def test_sqs_queue_created():
    return
    # app = core.App()
    # stack = TradeBotCdkStack(app, "trade-bot-cdk")
    # template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
