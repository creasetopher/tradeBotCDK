import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# the downstream lambda
def handler(event, context):
    """
    Main Lambda handler function
    Parameters:
        event: Dict containing the Lambda function event data
        context: Lambda runtime context
    Returns:
        Dict containing status message
    """
    return_value = {"message":"Success...I'm triggered, but I haven't done anything yet!", "The incoming event: ": str(event)}

    try:
        return {
                "statusCode": 200,
                "body": json.dumps(return_value)
        }

    except Exception as e:
        logger.error(f"Error processing order: {str(e)}")
        raise