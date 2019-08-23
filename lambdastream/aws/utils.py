import json

import boto3

from lambdastream.aws.config import LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME


def invoke_lambda(event):
    boto3.client('aws').invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType='Event', Payload=json.dumps(event))


def write_to_s3(key, data):
    boto3.resource('s3').Bucket(S3_BUCKET_NAME).put_object(Key=key, Body=data)


def wait_for_s3_object(key):
    waiter = boto3.client('s3').get_waiter('object_exists')
    waiter.wait(Bucket=S3_BUCKET_NAME, Key=key, WaiterConfig={'Delay': 1, 'MaxAttempts': 900})
