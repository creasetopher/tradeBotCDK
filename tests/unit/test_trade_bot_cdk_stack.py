from tempfile import template
from typing import Mapping, Any

import aws_cdk as cdk
from aws_cdk import assertions
import pytest
from tradebot.models.transport import MarketEventTransport

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


def _assert_sns_transport_config(
    template: assertions.Template,
    environment: dict,
    worker_task_role_id: str,

) -> None:
    assert "MARKET_EVENT_TOPIC_ARN" in environment
    assert "MARKET_EVENT_STREAM_NAME" not in environment

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "Roles": [{"Ref": worker_task_role_id}],
            "PolicyDocument": {
                "Statement": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Action": "sns:Publish",
                                "Effect": "Allow",
                            }
                        )
                    ]
                )
            },
        },
    )

def _assert_kinesis_transport_config(
    template: assertions.Template,
    environment: dict,
    worker_task_role_id: str,

) -> None:
    assert environment["MARKET_EVENT_TRANSPORT"] == MarketEventTransport.KINESIS.value
    assert "MARKET_EVENT_STREAM_NAME" in environment
    assert "MARKET_EVENT_TOPIC_ARN" not in environment

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "Roles": [{"Ref": worker_task_role_id}],
            "PolicyDocument": {
                "Statement": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Action": assertions.Match.array_with(
                                    [
                                        "kinesis:PutRecord",
                                        "kinesis:PutRecords",
                                    ]
                                ),
                                "Effect": "Allow",
                            }
                        )
                    ]
                )
            },
        },
    )


def _assert_log_transport_config(
    template: assertions.Template,
    environment: dict,
    worker_task_role_id: str,
) -> None:
    assert environment["MARKET_EVENT_TRANSPORT"] == MarketEventTransport.LOG.value
    assert "MARKET_EVENT_TOPIC_ARN" not in environment
    assert "MARKET_EVENT_STREAM_NAME" not in environment


    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "Roles": [{"Ref": worker_task_role_id}],
            "PolicyDocument": {
                "Statement": assertions.Match.not_(
                    assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {"Action": "sns:Publish"}
                            )
                        ]
                    )
                )
            },
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "Roles": [{"Ref": worker_task_role_id}],
            "PolicyDocument": {
                "Statement": assertions.Match.not_(
                    assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Action": assertions.Match.array_with(
                                        [
                                            "kinesis:PutRecord",
                                            "kinesis:PutRecords",
                                        ]
                                    )
                                }
                            )
                        ]
                    )
                )
            },
        },
    )


def _worker_configuration(
    transport: MarketEventTransport,
) -> dict[str, Any]:
    app = cdk.App()
    stack = TradeBotCdkStack(
        app,
        "TestStack",
        stage="test",
        market_event_transport=transport,
    )
    template = assertions.Template.from_stack(stack)

    ec2_task_definitions = list(template.find_resources("AWS::ECS::TaskDefinition").values())
    task_definition = ec2_task_definitions[0]
    # this is the role ID for the worker task, 
    # is needed in each test to assert we are only looking at IAM policies attached to the worker task, and not any other tasks in the stack
    task_role_id = task_definition["Properties"]["TaskRoleArn"]["Fn::GetAtt"][0]


    container_definitions = task_definition["Properties"]["ContainerDefinitions"][0]
    env_variables = container_definitions["Environment"]
    environment = {
        variable["Name"]: variable["Value"]
        for variable in env_variables
    }
    return {"template": template, "environment": environment, "task_role_id": task_role_id}

@pytest.mark.parametrize("transport", list(MarketEventTransport))
def test_worker_transport_config(transport: MarketEventTransport) -> None:
    config = _worker_configuration(transport)
    environment: dict[str, Any] = config["environment"]
    template: assertions.Template = config["template"]
    task_role_id: str = config["task_role_id"]

    assert environment["MARKET_EVENT_TRANSPORT"] == transport.value


    if transport == MarketEventTransport.SNS:
        _assert_sns_transport_config(template, environment, task_role_id)
    elif transport == MarketEventTransport.KINESIS:
        _assert_kinesis_transport_config(template, environment, task_role_id)
    elif transport == MarketEventTransport.LOG:
        _assert_log_transport_config(template, environment, task_role_id)
