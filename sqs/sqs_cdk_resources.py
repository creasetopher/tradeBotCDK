from typing import Any
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_iam as iam
import aws_cdk as cdk
import logging
import json

logger = logging.getLogger(__name__)

    
class TickerSqsQueue:
    queue: sqs.Queue = None

    def __init__(self, stack, queue_name, queue_attributes={}) -> sqs.Queue:
        
        if not queue_name.endswith('.fifo'):
                queue_name += '.fifo'

        try:
            queue = sqs.Queue(
                stack,
                id=queue_attributes.get('id', queue_name),
                queue_name=queue_attributes.get('queue_name', queue_name),
                fifo=queue_attributes.get('fifo', True),
                content_based_deduplication=queue_attributes.get('content_based_deduplication', False),
                visibility_timeout= queue_attributes.get('visibility_timeout', cdk.Duration.seconds(30)),
                max_message_size_bytes=4096,
                receive_message_wait_time=queue_attributes.get('receive_message_wait_time', cdk.Duration.seconds(10)),
            )
            self.queue = queue
            logger.info(f"Created SQS FIFO queue: {queue_name} with URL: {self.queue.queue_url}")

        except Exception as e:
            logger.error(f"Error creating SQS FIFO queue {queue_name}: {e}")
            raise e


    def add_access_policy_for_sns_topic(self, topic_arn: str):
        """
        Add the necessary access policy to an SQS queue, so
        it can receive messages from a topic.

        :param sqs_queue: The SQS queue resource.
        :param topic_arn: The ARN of the topic.
        :return: None.
        """
        try:


            self.queue.add_to_resource_policy(
                iam.PolicyStatement(
                    actions=["SQS:SendMessage"],
                    # principals=[iam.AnyPrincipal()],
                    principals=[iam.ServicePrincipal("sns.amazonaws.com")],
                    resources=[self.queue.queue_arn],
                    conditions={
                        "ArnLike": {"aws:SourceArn": topic_arn}
                    }
                )
            )
                
            logger.info("Added trust policy to the queue.")
        except Exception as error:
            logger.exception("Couldn't add trust policy to the queue!")
            raise error