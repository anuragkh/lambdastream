import json
import select
import socket
import sys

import boto3
import cloudpickle

from lambdastream.channels.jiffy.storage.compat import bytes_to_str, b
from lambdastream.config import LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME
from lambdastream.executor import Executor, executor


def operator_handler(event, context):
    operator_id = event.get('stream_operator')
    operator_in = operator_id + '.in'
    host = event.get('host')
    port = LambdaExecutor.SYNC_PORT

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
    sock.send(b('READY:{}'.format(operator_id)))
    msg = sock.recv(1024)
    if msg != b('RUN'):
        print('Aborting operator...')
    print('Running operator...')
    operator_out = operator.run()
    print('Outputting output to S3...')
    bucket.put_object(Key=operator.operator_id + '.out', Body=cloudpickle.dumps(operator_out))
    print('All done!')


class Lambda(object):
    def __init__(self, operator):
        self.operator = operator
        self.s3_bucket = boto3.resource('s3').Bucket(S3_BUCKET_NAME)
        self.lambda_client = boto3.client('lambda')

    def start(self):
        pickled = cloudpickle.dumps(self.operator)
        print('Writing pickled operator for {} to S3 ({} bytes)...'.format(self.operator.operator_id, len(pickled)))
        self.s3_bucket.put_object(Key=self.operator.operator_id + '.in', Body=pickled)
        e = dict(stream_operator=self.operator.operator_id, host=socket.gethostname())
        print('Invoking lambda with payload: {}...'.format(e))
        self.lambda_client.invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType='Event', Payload=json.dumps(e))

    def join(self):
        waiter = boto3.client('s3').get_waiter('object_exists')
        waiter.wait(Bucket=S3_BUCKET_NAME, Key=self.operator.operator_id, WaiterConfig={'Delay': 1, 'MaxAttempts': 900})


@executor('lambda')
class LambdaExecutor(Executor):
    SYNC_PORT = 11001

    def __init__(self):
        super(LambdaExecutor, self).__init__()

    def exec(self, dag):
        lambdas = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                lambda_handle = Lambda(operator)
                lambdas.append(lambda_handle)
                lambda_handle.start()

        print('Invoked {} lambdas, starting synchronization...'.format(len(lambdas)))
        self.synchronize_operators(len(lambdas))
        print('Synchronization complete, waiting for lambdas to finish...')

        for l in lambdas:
            l.join()

        print('All lambdas completed')

    @staticmethod
    def synchronize_operators(operator_count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(False)
        s.settimeout(300)
        try:
            s.bind((socket.gethostname(), LambdaExecutor.SYNC_PORT))
        except socket.error as ex:
            print('Bind failed: {}'.format(ex))
            sys.exit()
        s.listen(5)
        inputs = [s]
        outputs = []
        ready = []
        ids = set()
        run = True
        while run:
            readable, writable, exceptional = select.select(inputs, outputs, inputs)
            for r in readable:
                if r is s:
                    sock, address = r.accept()
                    sock.setblocking(False)
                    inputs.append(sock)
                else:
                    data = r.recv(4096)
                    msg = bytes_to_str(data.rstrip().lstrip())
                    if not data:
                        inputs.remove(r)
                        r.close()
                    else:
                        print('DEBUG: [{}]'.format(msg))
                        op = int(msg.split('READY:')[1])
                        print('... Operator={} ready ...'.format(op))
                        if op not in ids:
                            print('... Queuing function id={} ...'.format(op))
                            ids.add(op)
                            ready.append((op, r))
                            if len(ids) == operator_count:
                                run = False
                            else:
                                print('.. Progress {}/{}'.format(len(ids), operator_count))
                        else:
                            print('... Aborting function id={} ...'.format(op))
                            r.send(b('ABORT'))
                            inputs.remove(r)
                            r.close()

        print('.. Starting benchmark ..')
        ready.sort(key=lambda x: x[0])
        for op in range(operator_count):
            op, sock = ready[op]
            print('... Running Operator={} ...'.format(op))
            sock.send(b('RUN'))

        s.close()
