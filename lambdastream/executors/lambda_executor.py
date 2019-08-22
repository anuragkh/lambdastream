import codecs
import json
import logging
import cloudpickle
import boto3

from lambdastream.config import LOG_LEVEL, LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME
from lambdastream.executor import Executor, executor

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def operator_handler(event, context):
    pickled = event.get('stream_operator')
    operator = cloudpickle.loads(codecs.decode(pickled.encode(), 'base64'))
    out = operator.run()

    bucket = boto3.resource('s3').Bucket(event.get('bucket'))
    bucket.put_object(Key=operator.operator_id, Body=cloudpickle.dumps(out))


class Lambda(object):
    def __init__(self, operator, function_name, bucket):
        self.operator = operator
        self.function_name = function_name
        self.bucket = bucket

    def start(self):
        logging.warning('Function must exist before invocation')
        pickled = cloudpickle.dumps(self.operator)
        e = dict(stream_operator=cloudpickle.loads(codecs.decode(pickled.encode(), 'base64')).decode(),
                 bucket=self.bucket)
        boto3.client('lambda').invoke(FunctionName=self.function_name,
                                      InvocationType='Event',
                                      Payload=json.dumps(e))

    def join(self):
        waiter = boto3.client('s3').get_waiter('object_exists')
        waiter.wait(Bucket=self.bucket, Key=self.operator.operator_id, WaiterConfig={'Delay': 1, 'MaxAttempts': 900})


@executor('lambda')
class LambdaExecutor(Executor):
    def __init__(self):
        super(LambdaExecutor, self).__init__()

    def exec(self, dag):
        lambdas = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                lambda_handle = Lambda(operator, LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME)
                lambdas.append(lambda_handle)
                lambda_handle.start()

        for l in lambdas:
            l.join()
