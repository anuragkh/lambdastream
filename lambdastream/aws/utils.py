import json
import select
import socket
import sys

import boto3

from lambdastream.aws.config import LAMBDA_FUNCTION_NAME, S3_BUCKET_NAME, LAMBDA_SYNC_PORT


def invoke_lambda(event):
    boto3.client('lambda').invoke(FunctionName=LAMBDA_FUNCTION_NAME, InvocationType='Event', Payload=json.dumps(event))


def write_to_s3(key, data):
    boto3.resource('s3').Object(S3_BUCKET_NAME, key).put(Body=data)
    print('Wrote {} to s3'.format(key))


def read_from_s3(key):
    data = boto3.resource('s3').Object(S3_BUCKET_NAME, key).get()['Body'].read()
    print('Read {} from s3'.format(key))
    return data


def delete_from_s3(key, silent=True):
    try:
        boto3.resource('s3').Object(S3_BUCKET_NAME, key).delete()
        print('Deleted {} from s3'.format(key))
    except Exception as e:
        if not silent:
            raise e


def wait_for_s3_object(key):
    boto3.client('s3').get_waiter('object_exists').wait(Bucket=S3_BUCKET_NAME, Key=key,
                                                        WaiterConfig={'Delay': 1, 'MaxAttempts': 900})


def synchronize_operators(host, operator_count):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setblocking(False)
    s.settimeout(300)
    try:
        s.bind((host, LAMBDA_SYNC_PORT))
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
                msg = data.rstrip().lstrip()
                if not data:
                    inputs.remove(r)
                    r.close()
                else:
                    print('DEBUG: [{}]'.format(msg))
                    op = msg.split(b'READY:')[1]
                    print('... Operator={} ready ...'.format(op))
                    if op not in ids:
                        print('... Queuing Operator={} ...'.format(op))
                        ids.add(op)
                        ready.append((op, r))
                        if len(ids) == operator_count:
                            run = False
                        else:
                            print('.. Progress {}/{}'.format(len(ids), operator_count))
                    else:
                        print('... Aborting Operator={} ...'.format(op))
                        r.send(b'ABORT')
                        inputs.remove(r)
                        r.close()

    print('.. Starting benchmark ..')
    ready.sort(key=lambda x: x[0])
    for op in range(operator_count):
        op, sock = ready[op]
        print('... Running Operator={} ...'.format(op))
        sock.send(b'RUN')

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
                msg = data.rstrip().lstrip()
                if not data:
                    inputs.remove(r)
                    r.close()
                else:
                    print('DEBUG: [{}]'.format(msg))
                    op = msg.split(b'DONE:')[1]
                    print('... Operator={} done ...'.format(op))
                    if op in ids:
                        ids.remove(op)
                        if len(ids) == 0:
                            run = False
                        else:
                            print('.. Progress {}/{}'.format(operator_count - len(ids), operator_count))
                    else:
                        print('... Aborting Operator={} ...'.format(op))
                        r.send(b'ABORT')
                        inputs.remove(r)
                        r.close()
    print('.. All lambdas complete ..')
    s.close()
