from lambdastream.operator import RoundRobinPartitioner
from lambdastream.utils import random_string


def noop(*args):
    pass


class Stream(object):
    def __init__(self, ctx, name='stream' + random_string(), parallelism=1, partitioner_cls=RoundRobinPartitioner):
        self.ctx = ctx
        self.name = name
        self.parallelism = parallelism
        self.partitioner_cls = partitioner_cls
        self.stages = ctx.dag_builder

    def _add_stage(self, name, op_fn, parallelism, partitioner_cls):
        if parallelism is None:
            parallelism = self.parallelism
        if partitioner_cls is None:
            partitioner_cls = self.partitioner_cls
        self.stages.add_stage(name, op_fn, parallelism, partitioner_cls)

    def map(self, map_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('map', map_fn, parallelism, partitioner_cls)
        return self

    def flat_map(self, flat_map_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('flat_map', flat_map_fn, parallelism, partitioner_cls)
        return self

    def filter(self, filter_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('filter', filter_fn, parallelism, partitioner_cls)
        return self

    def key_by(self, selector_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('key_by', selector_fn, parallelism, partitioner_cls)
        return KeyedStream(self.ctx, self.name, self.parallelism, self.partitioner_cls)

    def reduce_by_key(self, reduce_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('reduce', reduce_fn, parallelism, partitioner_cls)
        return self

    def inspect(self, inspect_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('sink', inspect_fn, parallelism, partitioner_cls)
        return self

    def print(self, parallelism=None, partitioner_cls=None):
        return self.inspect(print, parallelism, partitioner_cls)

    def run(self):
        if self.stages.get_stage(-1).op_type != 'sink':
            self._add_stage('sink', noop, self.parallelism, self.partitioner_cls)
        dag, channels = self.stages.build()
        [channel.init() for channel in channels]
        self.ctx.executor.exec(dag)
        [channel.destroy() for channel in channels]


class KeyedStream(Stream):
    def __init__(self, ctx, name, parallelism, partitioner_cls):
        super(KeyedStream, self).__init__(ctx, name, parallelism, partitioner_cls)

    def reduce(self, reduce_fn, parallelism=None, partitioner_cls=None):
        self._add_stage('reduce', reduce_fn, parallelism, partitioner_cls)
        return self
