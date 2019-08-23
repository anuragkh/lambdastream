import json

import boto3

from lambdastream.aws.config import LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME


def invoke_lambda(event):
    boto3.client('lambda').invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType='Event', Payload=json.dumps(event))


def write_to_s3(key, data):
    boto3.resource('s3').Object(S3_BUCKET_NAME, key).put(Body=data)


def read_from_s3(key):
    return boto3.resource('s3').Object(S3_BUCKET_NAME, key).get()['Body'].read()


def wait_for_s3_object(key):
    boto3.client('s3').get_waiter('object_exists').wait(Bucket=S3_BUCKET_NAME, Key=key,
                                                        WaiterConfig={'Delay': 1, 'MaxAttempts': 900})
