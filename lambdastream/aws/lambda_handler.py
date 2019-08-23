import socket

import boto3
from cloudpickle import cloudpickle

from lambdastream.aws.config import LAMBDA_SYNC_PORT, S3_BUCKET_NAME


def operator_handler(event, context):
    operator_id = event.get('stream_operator')
    operator_in = operator_id + '.in'
    host = event.get('host')
    port = LAMBDA_SYNC_PORT

    print('Creating stream_operator from file: {}'.format(operator_in))
    bucket = boto3.resource('s3').Bucket(S3_BUCKET_NAME)
    operator_binary = bucket.get_object(Key=operator_in)
    operator = cloudpickle.loads(operator_binary)
    assert operator.operator_id == operator_id, "Loaded operator does not match provided operator"
    print('Successfully reconstructed {} object'.format(operator.__class__.__name__))

    print('Connecting to host: {}, port: {} for synchronization...')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    print('Sending READY message...')
    sock.send('READY:{}'.encode())
    msg = sock.recv(1024)
    if msg != b'RUN':
        print('Aborting operator...')
    print('Running operator...')
    operator_out = operator.run()
    print('Outputting output to S3...')
    bucket.put_object(Key=operator.operator_id + '.out', Body=cloudpickle.dumps(operator_out))
    print('All done!')
