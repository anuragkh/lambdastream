import socket

from cloudpickle import cloudpickle

from lambdastream.aws.config import LAMBDA_SYNC_PORT
from lambdastream.aws.utils import read_from_s3, write_to_s3


def operator_handler(event, context):
    operator_id = event.get('stream_operator')
    operator_in = operator_id + '.in'
    host = event.get('host')
    port = LAMBDA_SYNC_PORT

    print('Creating stream_operator from file: {}...'.format(operator_in))
    operator_binary = read_from_s3(operator_in)
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
    write_to_s3(operator.operator_id + '.out', cloudpickle.dumps(operator_out))
    print('All done!')
