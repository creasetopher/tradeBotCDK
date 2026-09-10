import aws_cdk as cdk
from aws_cdk import assertions

from trade_bot_cdk.trade_bot_cdk_stack import TradeBotCdkStack


def test_active_candidates_table_created():
    app = cdk.App()
    stack = TradeBotCdkStack(app, "TestStack", stage="test")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "KeySchema": assertions.Match.array_with(
                [
                    {
                        "AttributeName": "universe_id",
                        "KeyType": "HASH",
                    },
                    {
                        "AttributeName": "symbol",
                        "KeyType": "RANGE",
                    },
                ]
            )
        },
    )


def test_candidate_writer_uses_expected_handler():
    app = cdk.App()
    stack = TradeBotCdkStack(app, "TestStack", stage="test")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Handler": "candidate_ddb_writer.handler",
        },
    )


def test_market_event_topic_fans_out_to_writer_queue_with_dlq():
    app = cdk.App()
    stack = TradeBotCdkStack(app, "TestStack", stage="test")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::SNS::Topic",
        {
            "TopicName": "tradebot-test-market-events",
        },
    )
    template.has_resource_properties(
        "AWS::SQS::Queue",
        {
            "QueueName": "tradebot-test-market-event-writer",
            "RedrivePolicy": {
                "deadLetterTargetArn": assertions.Match.any_value(),
                "maxReceiveCount": 5,
            },
            "VisibilityTimeout": 900,
        },
    )
    template.has_resource_properties(
        "AWS::SQS::Queue",
        {
            "QueueName": "tradebot-test-market-event-writer-dlq",
            "MessageRetentionPeriod": 1209600,
        },
    )
    template.has_resource_properties(
        "AWS::SNS::Subscription",
        {
            "Protocol": "sqs",
            "RawMessageDelivery": True,
        },
    )
