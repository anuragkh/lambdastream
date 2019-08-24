import time

import msgpack
import numpy as np

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
        self.write_latencies = []

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
            if timestamp > 0:
                write_time = time.time() - timestamp
                self.write_latencies.append(write_time)
            self.num_flushes += 1
            self.num_records_seen += len(batch)
            self.num_records_since_timestamp += len(batch)

        timestamp = time.time()
        [q.put(msgpack.packb((timestamp, DONE_MARKER))) for q in self.out_queues]
        [q.flush() for q in self.out_queues]
        return self.num_records_seen / (time.time() - start_time), None, None, self.write_latencies

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
        self.read_latencies = []
        self.write_latencies = []
        self.upstream_count = upstream_count
        self.num_done_markers = 0
        self.num_records_seen = 0

    def run(self):
        self.in_queue.connect()
        for q in self.out_queues:
            q.connect()
        done = False
        start_time = time.time()
        while not done:
            read_begin = time.time()
            timestamp, batch = msgpack.unpackb(self.in_queue.get())
            if timestamp > 0:
                self.read_latencies.append(time.time() - read_begin)
            if DONE_MARKER == batch:
                self.num_done_markers += 1
                if self.num_done_markers == self.upstream_count:
                    done = True
            else:
                processed_batch = self.process_batch(timestamp, batch)
                self.num_records_seen += len(batch)
                for record in processed_batch:
                    write_begin = time.time()
                    self.out_queues[self.key(record)].put(msgpack.packb((timestamp, record)))
                    if timestamp > 0:
                        self.write_latencies.append(time.time() - write_begin)

            if done:
                for q in self.out_queues:
                    q.put(msgpack.packb((timestamp, DONE_MARKER)))
                    q.flush()
        return self.num_records_seen / (time.time() - start_time), None, self.read_latencies, self.write_latencies

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

    def run(self):
        throughput, _, read, write = super(Sink, self).run()
        return throughput, self.latencies, read, write
