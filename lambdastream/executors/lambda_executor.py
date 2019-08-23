import socket

import cloudpickle

from lambdastream.aws.utils import invoke_lambda, wait_for_s3_object, write_to_s3, synchronize_operators, read_from_s3
from lambdastream.executors.executor import Executor, executor


class Lambda(object):
    def __init__(self, operator, host):
        self.operator = operator
        self.host = host
        self.thput = None
        self.lat = None

    def start(self):
        pickled = cloudpickle.dumps(self.operator)
        print('Writing pickled operator for {} to S3 ({} bytes)...'.format(self.operator.operator_id, len(pickled)))
        write_to_s3(self.operator.operator_id + '.in', pickled)
        e = dict(stream_operator=self.operator.operator_id, host=self.host)
        print('Invoking aws with payload: {}...'.format(e))
        invoke_lambda(e)

    def join(self):
        wait_for_s3_object(self.operator.operator_id + '.out')
        self.thput, self.lat = cloudpickle.loads(read_from_s3(self.operator.operator_id + '.out'))

    def throughput(self):
        return self.thput

    def latency(self):
        return self.lat


@executor('aws_lambda')
class LambdaExecutor(Executor):
    def __init__(self, **kwargs):
        super(LambdaExecutor, self).__init__(**kwargs)
        self.host = kwargs.get('sync_host', socket.gethostname())

    def exec(self, dag):
        lambdas = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                lambda_handle = Lambda(operator, self.host)
                lambdas.append(lambda_handle)
                lambda_handle.start()

        print('Invoked {} lambdas, starting synchronization...'.format(len(lambdas)))
        synchronize_operators(self.host, len(lambdas))
        print('Synchronization complete, waiting for lambdas to finish...')

        for l in lambdas:
            l.join()

        print('All lambdas completed')
        return {l.operator.operator_id: l.throughput() for l in lambdas}, {l.operator.operator_id: l.latency() for l in
                                                                           lambdas if l.latency() is not None}
