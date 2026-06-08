from constructs import Construct
from aws_cdk import (
    aws_lambda as _lambda, 
    aws_dynamodb as ddb
)

TICKER_TABLE_NAME = 'TickerTable'
class TickerApi(Construct):

    @property
    def handler(self):
        return self._handler  


    @property
    def table(self):
        return self._table
    
    def __init__(self, scope: Construct, id: str, downstream_lambda: _lambda.IFunction, **kwargs):
        super().__init__(scope, id, **kwargs)


        self._table = ddb.Table(
                self, TICKER_TABLE_NAME,
                partition_key={'name': 'symbol', 'type': ddb.AttributeType.STRING},
                encryption=ddb.TableEncryption.AWS_MANAGED
            )

        self._handler = _lambda.Function(
            self, 'TickerApiHandler',
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler='ticker_api_handler.handler',
            code=_lambda.Code.from_asset('lambda'),
            environment={
                'DOWNSTREAM_FUNCTION_NAME': downstream_lambda.function_name,
                'TICKER_TABLE_NAME': self._table.table_name,
            }
        )

        self._table.grant_read_write_data(self._handler)
        downstream_lambda.grant_invoke(self._handler)

    