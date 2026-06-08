
from typing import Any
import uuid
import boto3
from botocore.exceptions import ClientError
import logging
import json

logger = logging.getLogger(__name__)
FIFO_SUFFIX = '.fifo'

class SnsWrapper:
    sns_client: Any = None
    """Wrapper class for managing Amazon SNS operations."""

    def __init__(self) -> None:
        """
        Initialize the SnsWrapper.

        This constructor sets up the SNS client using boto3."""
        self.sns_client = boto3.client('sns')


    def create_fifo_topic(
        self, 
        topic_name: str
    ) -> str:
        """
        Create an SNS FIFO topic.

        :param topic_name: The name of the topic to create.
        :return: The ARN of the created topic.
        :raises ClientError: If the topic creation fails.
        """
        try:
            # Add .fifo suffix for FIFO topics
            if not topic_name.endswith(FIFO_SUFFIX):
                topic_name += FIFO_SUFFIX


            topic = self.sns_client.create_topic(
                Name=topic_name,
                Attributes={
                    'FifoTopic': str(True),
                    'ContentBasedDeduplication': str(True)
                } 
            )

            topic_arn = topic['TopicArn']

            logger.info(f"Created topic: {topic_name} with ARN: {topic_arn}")
            logger.info(topic_arn)

            return topic_arn

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"Error creating topic {topic_name}: {error_code} - {e}")
            raise



    @staticmethod
    def add_access_policy_for_sqs(sqs_queue, topic_arn):
        """
        Add the necessary access policy to an SQS queue, so
        it can receive messages from a topic.

        :param sqs_queue: The SQS queue resource.
        :param topic_arn: The ARN of the topic.
        :return: None.
        """
        try:
            queue_url = sqs_queue["QueueUrl"]
            sqs_client = boto3.client('sqs')
            queue_arn = sqs_client.get_queue_attributes(
                QueueUrl = queue_url,
                AttributeNames = ['QueueArn']
            )["Attributes"]["QueueArn"]


            sqs_queue.set_attributes(
                Attributes={
                    "Policy": json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Sid": "test-sid",
                                    "Effect": "Allow",
                                    "Principal": {"AWS": "*"},
                                    "Action": "SQS:SendMessage",
                                    "Resource": queue_arn,
                                    "Condition": {
                                        "ArnLike": {"aws:SourceArn": topic_arn}
                                    },
                                }
                            ],
                        }
                    )
                }
            )
            logger.info("Added trust policy to the queue.")
        except ClientError as error:
            logger.exception("Couldn't add trust policy to the queue!")
            raise error
        

    def subscribe_sqs_queue_to_topic(self, topic_arn, sqs_queue_arn):
        """
        Subscribe an SQS queue to a topic.

        :param topic: The topic resource.
        :param queue_arn: The ARN of the SQS queue.
        :return: The subscription ARN.
        """
        try:
            subscription_arn = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=sqs_queue_arn,
                ReturnSubscriptionArn=True,
            )["SubscriptionArn"]
            logger.info("The SQS queue is subscribed to the topic.")
            return subscription_arn
        except ClientError as error:
            logger.exception("Couldn't subscribe SQS queue to topic!")
            raise error



    def publish_stock_update(self, topic_arn, payload, group_id):
        """
        Compose and publish a message with stock update information.

        :param topic_arn: The ARN of the topic to publish to.
        :param payload: The message to publish.
        :param group_id: The group ID for the message.
        :return: The ID of the message.
        """
        try:
            message_attributes = {"stock_change": {"DataType": "String", "StringValue": "update"}}
            dedup_id = uuid.uuid4()
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Subject="Stock Update",
                Message=payload,
                MessageAttributes=message_attributes,
                MessageGroupId=group_id,
                MessageDeduplicationId=str(dedup_id),
            )
            message_id = response["MessageId"]
            logger.info("response: %s.", str(response))
            logger.info("Published message to topic %s.", topic_arn)
        except ClientError as error:
            logger.exception("Couldn't publish message to topic %s.", topic_arn)
            raise error
        return message_id

    def delete_topic(self, topic_arn):
        """
        Deletes a topic. All subscriptions to the topic are also deleted.
        """
        try:
            self.sns_client.delete_topic(TopicArn=topic_arn)
            logger.info("Deleted topic %s.", topic_arn)
        except ClientError:
            logger.exception("Couldn't delete topic %s.", topic_arn)
            raise

    

    def unsubscribe(self, subscription_arn):
        """
        Unsubscribes and deletes a subscription.
        """
        try:
            self.sns_client.unsubscribe(SubscriptionArn=subscription_arn)
            logger.info("Deleted subscription %s.", subscription_arn)
        except ClientError:
            logger.exception("Couldn't delete subscription %s.", subscription_arn)
            raise

    
    def list_subscriptions(self, topic=None):
        """
        Lists subscriptions for the current account, optionally limited to a
        specific topic.

        :param topic: When specified, only subscriptions to this topic are returned.
        :return: An iterator that yields the subscriptions.
        """
        try:
            if topic is None:
                subs_iter = self.sns_client.subscriptions.all()
            else:
                subs_iter = topic.subscriptions.all()
            logger.info("Got subscriptions.")
        except ClientError:
            logger.exception("Couldn't fetch subscriptions.")
            raise
        else:
            return subs_iter
        
    def list_topics(self):
        """
        Lists topics for the current account.

        :return: An iterator that yields the topics.
        """
        try:
            topics = self.sns_client.topics.all()
            logger.info("Got topics.")
        except ClientError:
            logger.exception("Couldn't fetch topics.")
            raise
        else:
            return topics
        

    
    @staticmethod
    def publish_message(topic, message, attributes):
        """
        Publishes a message, with attributes, to a topic. Subscriptions can be filtered
        based on message attributes so that a subscription receives messages only
        when specified attributes are present.

        :param topic: The topic to publish to.
        :param message: The message to publish.
        :param attributes: The key-value attributes to attach to the message. Values
                           must be either `str` or `bytes`.
        :return: The ID of the message.
        """
        try:
            attributes = {}
            for key, value in attributes.items():
                if isinstance(value, str):
                    attributes[key] = {"DataType": "String", "StringValue": value}
                elif isinstance(value, bytes):
                    attributes[key] = {"DataType": "Binary", "BinaryValue": value}
            response = topic.publish(Message=message, MessageAttributes=attributes)
            message_id = response["MessageId"]
            logger.info(
                "Published message with attributes %s to topic %s.",
                attributes,
                topic.attributes["TopicArn"],
            )
        except ClientError:
            logger.exception("Couldn't publish message to topic %s.", topic.attributes["TopicArn"])
            raise
        else:
            return message_id
        
    def publish_text_message(self, phone_number, message):
        """
        Publishes a text message directly to a phone number without need for a
        subscription.

        :param phone_number: The phone number that receives the message. This must be
                             in E.164 format. For example, a United States phone
                             number might be +12065550101.
        :param message: The message to send.
        :return: The ID of the message.
        """
        try:
            response = self.sns_client.meta.client.publish(
                PhoneNumber=phone_number, Message=message
            )
            message_id = response["MessageId"]
            logger.info("Published text message to %s.", phone_number)
        except ClientError:
            logger.exception("Couldn't publish text message to %s.", phone_number)
            raise
        else:
            return message_id