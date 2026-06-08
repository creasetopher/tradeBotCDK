import aws_cdk.aws_sns as sns
import aws_cdk.aws_iam as iam
import aws_cdk.aws_sns_subscriptions as sns_subs
from aws_cdk.aws_sqs import Queue
import logging


FIFO_SUFFIX = '.fifo'

logger = logging.getLogger(__name__)

class TickerSnsTopic:
    """Wrapper class for managing Amazon SNS operations."""

    topic: sns.Topic = None

    def __init__(self, stack, topic_name) -> None:
        
        if not topic_name.endswith(FIFO_SUFFIX):
            topic_name += FIFO_SUFFIX

        try:
            topic = sns.Topic(
                stack,
                id=topic_name,
                content_based_deduplication=True,
                display_name=topic_name,
                enforce_ssl=True,
                fifo=True,
            )


            topic_arn = topic.topic_arn

            logger.info(f"Created topic: {topic_name} with ARN: {topic_arn}")
            logger.info(topic_arn)

            self.topic = topic

        except Exception as e:
            logger.error(f"Error creating topic {topic_name}: {e}")
            raise e
    

    def subscribe_queue(self, queue: Queue):
        """
        Subscribes an SQS queue to the SNS topic.

        :param queue_arn: The ARN of the SQS queue to subscribe.
        :raises ClientError: If the subscription fails.
        """
        try:
            sub = sns_subs.SqsSubscription(queue)
            return sub
        except Exception as e:
            logger.error(f"Error subscribing SQS queue with ARN {queue.queue_arn} to topic {self.topic.topic_name}: {e}")


    def grant_publish_permission(self, principal_arn: str):
        """
        Grants permission to publish messages to the SNS topic.

        :param principal_arn: The ARN of the principal (e.g., IAM role) to grant permission to.
        :raises ClientError: If the permission grant fails.
        """
        try:
            self.topic.grant_publish(iam.ArnPrincipal(principal_arn))
            logger.info(f"Granted publish permission to {principal_arn} for topic {self.topic.topic_name}.")
        except Exception as e:
            logger.error(f"Error granting publish permission to {principal_arn} for topic {self.topic.topic_name}: {e}")