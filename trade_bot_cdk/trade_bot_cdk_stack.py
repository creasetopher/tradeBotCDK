from aws_cdk import (
    # Duration,
    Stack,
    aws_apigateway as apigw,
    # aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_s3 as _s3,
    CfnOutput
)
from constructs import Construct
from ticker_api.ticker_api import TickerApi
from cdk_dynamo_table_view import TableViewer
from sns.sns_cdk_resources import TickerSnsTopic
from sqs.sqs_cdk_resources import TickerSqsQueue


class TradeBotCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "TradeBotCdkQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )


        lambda_function = _lambda.Function(
            self, "TradeBotLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="trade_bot_lambda_handler.handler",
            code=_lambda.Code.from_asset("lambda"),
        )

        # function_url = lambda_function.add_function_url(
        #     auth_type=_lambda.FunctionUrlAuthType.NONE
        # )

        # endpoint = apigw.LambdaRestApi(
        #     self,
        #     "ApiGwEndpoint",
        #     handler=lambda_function,
        #     rest_api_name="TradeBotApi"
        # )

        # ticker_api = TickerApi(self, "TickerApi", downstream_lambda=lambda_function)
        

        # TableViewer(
        #     self, 'TickerHitCounter',
        #     title='Ticker Hits',
        #     table=ticker_api._table
        # )

        # apigw.LambdaRestApi(
        #     self,
        #     "TickerApiEndpoint",
        #     handler=ticker_api.handler,
        #     rest_api_name="TickerApi"
        # )



        # Output the function URL
        # CfnOutput(self, "TradeBotLambdaFunctionURL", value=function_url.url)

        sns_wrapper = TickerSnsTopic(self, "TickerUpdates")
        sqs_wrapper = TickerSqsQueue(self, "TickerUpdatesQueue")

        sqs_queue = sqs_wrapper.queue
        sns_topic = sns_wrapper.topic

        sqs_wrapper.add_access_policy_for_sns_topic(sns_topic.topic_arn)
        sns_wrapper.subscribe_queue(sqs_queue)

        sns_wrapper.grant_publish_permission(lambda_function.function_arn)        

        
