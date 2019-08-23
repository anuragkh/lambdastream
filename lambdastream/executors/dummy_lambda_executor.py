import socket
import sys
from multiprocessing import Process

import cloudpickle

from lambdastream.aws.lambda_handler import operator_handler
from lambdastream.aws.utils import wait_for_s3_object, write_to_s3, synchronize_operators, read_from_s3
from lambdastream.executors.executor import Executor, executor


def dummy_handler(event, context):
    sys.stdout = open(event.get('stream_operator') + ".out", "a", buffering=1)
    sys.stderr = open(event.get('stream_operator') + ".err", "a", buffering=1)
    return operator_handler(event, context)


class DummyLambda(object):
    def __init__(self, operator, host):
        self.operator = operator
        self.host = host
        self.handle = None

    def start(self):
        pickled = cloudpickle.dumps(self.operator)
        print('Writing pickled operator for {} to S3 ({} bytes)...'.format(self.operator.operator_id, len(pickled)))
        write_to_s3(self.operator.operator_id + '.in', pickled)
        e = dict(stream_operator=self.operator.operator_id, host=self.host)
        print('Invoking aws with payload: {}...'.format(e))
        self.handle = Process(target=dummy_handler, args=(e, None,))
        self.handle.start()

    def join(self):
        wait_for_s3_object(self.operator.operator_id + '.out')
        self.handle.join()

    def throughput(self):
        return cloudpickle.loads(read_from_s3(self.operator.operator_id + '.out'))


@executor('dummy_lambda')
class DummyLambdaExecutor(Executor):
    def __init__(self, **kwargs):
        super(DummyLambdaExecutor, self).__init__(**kwargs)
        self.host = kwargs.get('sync_host', socket.gethostname())

    def exec(self, dag):
        sync_worker = Process(target=synchronize_operators, args=(self.host, sum(map(len, dag))))
        sync_worker.start()
        lambdas = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                lambda_handle = DummyLambda(operator, self.host)
                lambdas.append(lambda_handle)
                lambda_handle.start()

        print('Invoked {} lambdas, waiting for synchronization...'.format(len(lambdas)))
        sync_worker.join()
        print('Synchronization complete, waiting for lambdas to finish...')

        for l in lambdas:
            l.join()

        print('All lambdas completed')
        return {l.operator.operator_id: l.throughput() for l in lambdas}
