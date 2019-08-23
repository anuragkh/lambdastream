import socket

from cloudpickle import cloudpickle

from lambdastream.aws.config import LAMBDA_SYNC_PORT
from lambdastream.aws.s3_backend import S3Backend


def operator_handler(event, context):
    operator_id = event.get('stream_operator')
    operator_in = operator_id + '.in'
    s3 = S3Backend()
    host = event.get('host')
    port = LAMBDA_SYNC_PORT

    print('Creating stream_operator from file: {}...'.format(operator_in))
    operator_binary = s3.get_object(operator_in)
    print('Read data from file: {}, unpickling...'.format(operator_in))
    operator = cloudpickle.loads(operator_binary)
    assert operator.operator_id == operator_id, 'Loaded operator does not match provided operator'
    print('Successfully reconstructed {} object'.format(operator.__class__.__name__))

    print('Connecting to host: {}, port: {} for synchronization...'.format(host, port))
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
    s3.put_object(operator.operator_id + '.out', cloudpickle.dumps(operator_out))
    print('All done!')
