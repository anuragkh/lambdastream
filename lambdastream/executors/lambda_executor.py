import select
import socket
import sys

import cloudpickle

from lambdastream.aws.config import LAMBDA_SYNC_PORT
from lambdastream.aws.utils import invoke_lambda, write_to_s3, wait_for_s3_object
from lambdastream.executors.executor import Executor, executor


class Lambda(object):
    def __init__(self, operator):
        self.operator = operator

    def start(self):
        pickled = cloudpickle.dumps(self.operator)
        print('Writing pickled operator for {} to S3 ({} bytes)...'.format(self.operator.operator_id, len(pickled)))
        write_to_s3(self.operator.operator_id + '.in', pickled)
        e = dict(stream_operator=self.operator.operator_id, host=socket.gethostname())
        print('Invoking aws with payload: {}...'.format(e))
        invoke_lambda(e)

    def join(self):
        wait_for_s3_object(self.operator.operator_id + '.out')


@executor('aws_lambda')
class LambdaExecutor(Executor):
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
            s.bind((socket.gethostname(), LAMBDA_SYNC_PORT))
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
                    msg = data.rstrip().lstrip().encode()
                    if not data:
                        inputs.remove(r)
                        r.close()
                    else:
                        print('DEBUG: [{}]'.format(msg))
                        op = int(msg.split(b'READY:')[1])
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
                            r.send(b'ABORT')
                            inputs.remove(r)
                            r.close()

        print('.. Starting benchmark ..')
        ready.sort(key=lambda x: x[0])
        for op in range(operator_count):
            op, sock = ready[op]
            print('... Running Operator={} ...'.format(op))
            sock.send(b'RUN')

        s.close()
