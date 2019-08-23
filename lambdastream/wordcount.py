import string
import time

import msgpack
import numpy as np

from lambdastream.channels.channel import REGISTERED_CHANNELS, ChannelBuilder
from lambdastream.constants import DONE_MARKER
from lambdastream.wordcount_ops import cython_process_batch_reduce, cython_process_batch_map

SENTENCE_LENGTH = 100
WORDS = {}


class WordSource(object):
    def __init__(self, idx, op_id, out_queue_ids, batch_size, channel_builder, words_file, timestamp_interval,
                 num_records):
        self.idx = idx
        self.operator_id = op_id

        with open(words_file) as f:
            self.words = []
            for line in f.readlines():
                self.words.append(line.strip())
        self.words = np.array([word.encode('ascii') for word in self.words])
        self.timestamp_interval = timestamp_interval

        self.batch_size = batch_size
        self.out_queues = [channel_builder.build_output_channel(name) for name in out_queue_ids]
        self.num_records = num_records

        # Number of records
        self.num_flushes = 0
        self.num_records_seen = 0
        self.num_records_since_timestamp = 0

        # Set the seed so that we can deterministically generate the sentences.
        np.random.seed(0)

    def get_batch(self, batch_size):
        return [np.string_.join(b' ', np.random.choice(self.words, SENTENCE_LENGTH)) for _ in range(batch_size)]

    def run(self):
        for q in self.out_queues:
            q.connect()
        start_time = time.time()
        while self.num_records_seen < self.num_records:
            batch = self.get_batch(self.batch_size)
            assert (len(batch) > 0), len(batch)
            timestamp = 0
            if self.num_records_since_timestamp > self.timestamp_interval:
                timestamp = time.time()
                self.num_records_since_timestamp -= self.timestamp_interval
            self.out_queues[self.num_flushes % len(self.out_queues)].put(msgpack.packb((timestamp, batch)))
            self.num_flushes += 1
            self.num_records_seen += len(batch)
            self.num_records_since_timestamp += len(batch)

        timestamp = time.time()
        [q.put(msgpack.packb((timestamp, DONE_MARKER))) for q in self.out_queues]
        [q.flush() for q in self.out_queues]
        return self.num_records_seen / (time.time() - start_time)  # Throughput

    def num_records_seen(self):
        return self.num_records_seen

    def ping(self):
        return


class StreamOperator(object):
    def __init__(self, idx, op_id, out_op_ids, upstream_count, channel_builder):
        self.idx = idx
        self.operator_id = op_id
        self.in_queue = channel_builder.build_input_channel(op_id)
        self.out_queues = [channel_builder.build_output_channel(name) for name in out_op_ids]
        self.num_out = len(self.out_queues)
        self.upstream_count = upstream_count
        self.num_done_markers = 0
        self.num_records_seen = 0

    def run(self):
        self.in_queue.connect()
        for q in self.out_queues:
            q.connect()
        done = False
        while not done:
            timestamp, batch = msgpack.unpackb(self.in_queue.get())
            if DONE_MARKER == batch:
                self.num_done_markers += 1
                if self.num_done_markers == self.upstream_count:
                    done = True
            else:
                processed_batch = self.process_batch(timestamp, batch)
                self.num_records_seen += len(batch)
                for record in processed_batch:
                    self.out_queues[self.key(record)].put(msgpack.packb((timestamp, record)))

            if done:
                for q in self.out_queues:
                    q.put(msgpack.packb((timestamp, DONE_MARKER)))
                    q.flush()

    def ping(self):
        return

    def num_records_seen(self):
        return self.num_records_seen

    def process_batch(self, timestamp, batch):
        raise NotImplementedError()

    def key(self, record):
        raise NotImplementedError()


class Mapper(StreamOperator):
    def __init__(self, *args):
        import gc
        gc.disable()
        super().__init__(*args)
        self.counter = 0

    def key(self, record):
        self.counter += 1
        return self.counter % self.num_out

    def process_batch(self, timestamp, batch):
        return cython_process_batch_map(batch, self.num_out)


class Reducer(StreamOperator):
    def __init__(self, *args):
        super().__init__(*args)
        self.state = {}
        self.counter = 0

    def key(self, record):
        return self.idx

    def process_batch(self, timestamp, records):
        if len(records) != 1:
            print(len(records))
        assert len(records) == 1
        sink_output = []
        word = cython_process_batch_reduce(self.state, records[0])
        sink_output.append([word])
        return sink_output

    def get_counts(self):
        return self.state


class Sink(StreamOperator):
    def __init__(self, *args):
        super().__init__(*args)
        self.latencies = []

    def key(self, record):
        return 0

    def process_batch(self, timestamp, batch):
        if timestamp > 0:
            self.latencies.append(time.time() - timestamp)
        return []

    def flush_latencies(self):
        return self.latencies


def build_dag(**kwargs):
    channel = kwargs.get('channels', 'redis')
    num_mappers = kwargs.get('num_mappers', 1)
    num_reducers = kwargs.get('num_reducers', 1)
    batch_size = kwargs.get('batch_size', 64)
    words_file = kwargs.get('words_file')
    timestamp_interval = kwargs.get('timestamp_interval', 64)
    num_records = kwargs.get('num_records')

    input_channel_cls = REGISTERED_CHANNELS[channel]['input_channel']
    output_channel_cls = REGISTERED_CHANNELS[channel]['output_channel']
    channel_ctx_cls = REGISTERED_CHANNELS[channel]['channel_context']
    channel_builder = ChannelBuilder(channel_ctx_cls, input_channel_cls, output_channel_cls, **kwargs)

    operator_ids = [a + b for a in list(string.ascii_uppercase) for b in list(string.ascii_uppercase)]
    # One source per mapper.
    source_keys = [operator_ids.pop(0) for _ in range(num_mappers)]
    mapper_keys = [operator_ids.pop(0) for _ in range(num_mappers)]
    reducer_keys = [operator_ids.pop(0) for _ in range(num_reducers)]
    sink_keys = [operator_ids.pop(0) for _ in range(num_reducers)]
    all_keys = sink_keys + reducer_keys + mapper_keys + source_keys

    # Create the sink.
    sinks = []
    for i, sink_key in enumerate(sink_keys):
        sink_args = [i, sink_key, [], num_reducers, channel_builder]
        print("Creating sink", sink_key)
        sinks.append(Sink(*sink_args))

    # Create the reducers.
    reducers = []
    for i, reducer_key in enumerate(reducer_keys):
        reducer_args = [i, reducer_key, sink_keys, num_mappers, channel_builder]
        print("Creating reducer", reducer_key, "downstream:", sink_keys)
        reducers.append(Reducer(*reducer_args))

    # Create the intermediate operators.
    mappers = []
    for i, mapper_key in enumerate(mapper_keys):
        mapper_args = [i, mapper_key, reducer_keys, num_mappers, channel_builder]
        print("Creating mapper", mapper_key, "downstream:", reducer_keys)
        mappers.append(Mapper(*mapper_args))

    # Create the sources.
    sources = []
    for i, source_key in enumerate(source_keys):
        source_args = [i, source_key, mapper_keys, batch_size, channel_builder, words_file, timestamp_interval,
                       num_records]
        print("Creating source", source_key, "downstream:", mapper_keys)
        sources.append(WordSource(*source_args))

    return [sinks, reducers, mappers, sources], [channel_builder.build_channel_ctx(key) for key in all_keys]
