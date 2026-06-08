import boto3
from typing import Any
import logging
from botocore.exceptions import ClientError
import json

logger = logging.getLogger(__name__)

class Queue:
    def __init__(self, queue_url: str, queue_arn: str, attributes: dict[str, Any] = {}):
        self.queue_url = queue_url
        self.queue_arn = queue_arn
        self.attributes = attributes

    def __str__(self):
        return json.dumps({
            "queue_url": self.queue_url,
            "queue_arn": self.queue_arn,
            "attributes": self.attributes
        })
    
class SqsWrapper:
    sqs_client: Any = None
    """Wrapper class for managing Amazon SQS operations."""

    def __init__(self) -> None:
        """
        Initialize the SqsWrapper.

        This constructor sets up the SQS client using boto3."""
        self.sqs_client = boto3.client('sqs')

    def create_fifo_queue(
        self, 
        queue_name: str
    ) -> Queue:
        """
        Create an SQS FIFO queue.

        :param queue_name: The name of the queue to create.
        :return: The ARN of the created queue.
        :raises ClientError: If the queue creation fails.
        """
        try:
            # Add .fifo suffix for FIFO queues
            if not queue_name.endswith('.fifo'):
                queue_name += '.fifo'

            response = self.sqs_client.create_queue(
                QueueName=queue_name,
                Attributes={
                    'FifoQueue': str(True),
                    'ContentBasedDeduplication': str(False),
                    "MaximumMessageSize": str(4096),
                    "ReceiveMessageWaitTimeSeconds": str(10),
                }
            )

            queue_url = response['QueueUrl']
            queue: Queue = self.get_queue(queue_url)

            logger.info(f"queue: {queue}")
            logger.info(f"Created SQS FIFO queue: {queue_name} with URL: {queue_url}")
            return queue

        except (ClientError, Exception) as e:
            if type(e) == self.sqs_client.exceptions.QueueNameExists:
                logger.warning(f"Queue {queue_name} already exists. Retrieving existing queue URL.")
                try:
                    queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
                    queue = self.get_queue(queue_url)
                    logger.info(f"Retrieved existing queue URL: {queue_url}")
                    return queue
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                    logger.error(f"Error retrieving existing queue {queue_name}: {error_code} - {e}")
                    raise
            elif type(e) == ClientError:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                logger.error(f"ClientError creating SQS FIFO queue {queue_name}: {error_code} - {e}")
                raise
            raise e

    def get_queue(self, queue_url: str) -> Queue:
        """
        Gets an SQS queue by URL.

        :param queue_url: The URL of the queue to retrieve.
        :return: A Queue object.
        """
        if self.sqs_client is None:
            raise ValueError("SQS client is not initialized.")
        try:
            queue_info = self.sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['All'])
            queue = Queue(queue_url, queue_info['Attributes']['QueueArn'], queue_info['Attributes'])
            logger.info("Got queue with URL=%s", queue_url)
            return queue
        except ClientError as error:
            logger.exception("Couldn't get queue with URL %s.", queue_url)
            raise error
                
    def remove_queue(self, queue_url: str):
        """
        Removes an SQS queue. When run against an AWS account, it can take up to
        60 seconds before the queue is actually deleted.

        :param queue_url: The URL of the queue to delete.
        :return: None
        """
        try:
            self.sqs_client.delete_queue(QueueUrl=queue_url)
            logger.info("Deleted queue with URL=%s.", queue_url)
        except ClientError as error:
            logger.exception("Couldn't delete queue with URL=%s!", queue_url)
            raise error
        except Exception as error:
            logger.exception("Couldn't delete queue. Does queue exist?=%s!", queue_url)
            raise error
        

    def add_access_policy_for_sns_topic(self, queue: Queue, topic_arn: str):
        """
        Add the necessary access policy to an SQS queue, so
        it can receive messages from a topic.

        :param sqs_queue: The SQS queue resource.
        :param topic_arn: The ARN of the topic.
        :return: None.
        """
        try:

            self.sqs_client.set_queue_attributes(
                QueueUrl=queue.queue_url,
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
                                    "Resource": queue.queue_arn,
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