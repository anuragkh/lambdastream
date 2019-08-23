import logging

from operator.channels.channel import ChannelBuilder, REGISTERED_CHANNELS
from lambdastream.config import LOG_LEVEL
from lambdastream.dag import DAGBuilder
from lambdastream.executors.executor import REGISTERED_EXECUTORS
from lambdastream.stream import Stream
from operator.operator import RoundRobinPartitioner

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


class StreamContext(object):
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)

        executor = kwargs.get('executor', 'local')
        self.executor = REGISTERED_EXECUTORS[executor]()
        self.channel_args = kwargs

        channel = kwargs.get('channels', 'local')
        if channel == 'local':
            if executor != 'local':
                raise ValueError('local channels can only be used with local executor, provided: {}'.format(executor))
            self.channel_args['queue_dict'] = dict()

        self.batch_size = kwargs.setdefault('batch_size', 1)
        self.input_channel_cls = REGISTERED_CHANNELS[channel]['input_channel']
        self.output_channel_cls = REGISTERED_CHANNELS[channel]['output_channel']
        self.channel_ctx_cls = REGISTERED_CHANNELS[channel]['channel_context']
        self.channel_builder = ChannelBuilder(self.channel_ctx_cls, self.input_channel_cls, self.output_channel_cls,
                                              **self.channel_args)
        self.dag_builder = DAGBuilder(self.channel_builder)

    def create_stream(self, name, generator_fn, **kwargs):
        batch_size = kwargs.get('batch_size', self.batch_size)
        parallelism = kwargs.get('parallelism', 1)
        partitioner_cls = kwargs.get('partitioner', RoundRobinPartitioner)
        self.dag_builder.add_stage('source', generator_fn, parallelism, batch_size, partitioner_cls)
        return Stream(self, name, parallelism, batch_size, partitioner_cls)

    def custom_source(self, stream_name, source_cls, **kwargs):
        batch_size = kwargs.get('batch_size', self.batch_size)
        parallelism = kwargs.get('parallelism', 1)
        partitioner_cls = kwargs.get('partitioner', RoundRobinPartitioner)
        source_name = kwargs.get('source_name', 'custom_source')
        self.dag_builder.add_custom_stage(source_name, parallelism, source_cls, **kwargs)
        return Stream(self, stream_name, parallelism, batch_size, partitioner_cls)
