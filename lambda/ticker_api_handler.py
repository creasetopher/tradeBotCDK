import json
import os
import boto3
import utils.utils as utils


TEST_SYMBOL = 'AKAN'
def handler(event, context):
    ddb = boto3.resource('dynamodb')
    table = ddb.Table(os.environ['TICKER_TABLE_NAME'])
    _lambda = boto3.client('lambda')
    print('request: {}'.format(json.dumps(event)))

    # increment the hit counter for the symbol in DynamoDB
    table.update_item(
        Key={'symbol': TEST_SYMBOL},
        UpdateExpression='ADD hits :incr',
        ExpressionAttributeValues={':incr': 1}
    )


    # invoke the downstream lambda function and pass the event data to it
    resp = _lambda.invoke(
        FunctionName=os.environ['DOWNSTREAM_FUNCTION_NAME'],
        Payload=json.dumps(event),
    )

    # the response from invoke is a stream, so we need to read it and parse it as JSON
    body = resp['Payload'].read()

    print('downstream response: {}'.format(body))


    return json.loads(body)


def on_update_callback(message: str):
    print(f"Update received in Lambda: {message}\n\n")