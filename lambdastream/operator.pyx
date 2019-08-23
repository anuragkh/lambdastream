import logging
import time
from abc import ABC, abstractmethod

import msgpack

from lambdastream.config import LOG_LEVEL
from lambdastream.constants import DONE_MARKER, TIMESTAMP_INTERVAL
from cpython.ref cimport PyObject
from cpython.dict cimport PyDict_GetItem

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


class Partitioner(object):
    def __init__(self, num_out):
        self.num_out = num_out

    def partition(self, record):
        return 0

    def partition_batch(self, output_buffers, record_batch):
        for record in record_batch:
            output_buffers.append(self.partition(record), record)


class RoundRobinPartitioner(Partitioner):
    def __init__(self, num_out):
        super(RoundRobinPartitioner, self).__init__(num_out)
        self.i = 0

    def partition(self, record):
        p = self.i % self.num_out
        self.i += 1
        return p

    def partition_batch(self, output_buffers, record_batch):
        for record in record_batch:
            output_buffers.append(self.i % self.num_out, record)
            self.i += 1


class HashPartitioner(Partitioner):
    def __init__(self, num_out):
        super(HashPartitioner, self).__init__(num_out)

    def partition(self, record):
        return hash(record) % self.num_out

    def partition_batch(self, output_buffers, record_batch):
        cdef int h
        for record in record_batch:
            h = hash(record) % self.num_out
            output_buffers.append(h, record)


class OutputBuffers(object):
    def __init__(self, batch_size, output):
        self.batch_size = batch_size
        self.output = output
        self.num_out = len(output)
        self.output_buffers = [[] for _ in range(self.num_out)]
        self.timestamp = 0

    def set_timestamp(self, timestamp):
        self.timestamp = timestamp

    def append(self, idx, record):
        self.output_buffers[idx].append(record)
        if len(self.output_buffers[idx]) == self.batch_size:
            self.output[idx].put(msgpack.packb((self.timestamp, self.output_buffers[idx])))
            self.output_buffers[idx] = []
            self.timestamp = 0

    def close(self):
        for idx in range(self.num_out):
            if len(self.output_buffers[idx]) > 0:
                self.output[idx].put(msgpack.packb((self.timestamp, self.output_buffers[idx])))
            self.output[idx].put(msgpack.packb((self.timestamp, DONE_MARKER)))


REGISTERED_OPERATORS = {}

def operator(name):
    def register(operator_cls):
        logging.info('Registered operator class {} for {}'.format(operator_cls, name))
        REGISTERED_OPERATORS[name] = operator_cls
        return operator_cls
    return register


class Operator(ABC):
    def __init__(self, idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(LOG_LEVEL)
        self.idx = idx
        self.op_fn = operator_fn
        self.partitioner = partitioner
        self.operator_id = operator_id
        self.out_operator_ids = out_operator_ids
        self.num_out = len(self.out_operator_ids)
        self.upstream_count = upstream_count
        self.channel_builder = channel_builder
        self.batch_size = batch_size
        self.num_processed = 0

    @abstractmethod
    def run(self):
        pass


@operator('source')
class Source(Operator):
    def __init__(self, idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(Source, self).__init__(idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count,
                                     batch_size, channel_builder)
        self.num_batches = 0
        self.output = [channel_builder.build_output_channel(oid) for oid in out_operator_ids]
        self.output_buffers = OutputBuffers(self.batch_size, self.output)
        self.logger.info(
            'Created source {} with downstream operators: {}'.format(self.operator_id, self.out_operator_ids))

    def run(self):
        done = False
        for q in self.output:
            q.connect()

        start_time = time.time()
        while not done:
            record_batch = self.op_fn(self.batch_size)
            timestamp = 0
            if self.num_batches % TIMESTAMP_INTERVAL == 0:
                timestamp = time.time()
            self.output_buffers.set_timestamp(timestamp)
            if record_batch is DONE_MARKER:
                done = True
                self.output_buffers.close()
            else:
                self.partitioner.partition_batch(self.output_buffers, record_batch)
                self.num_processed += len(record_batch)
                self.num_batches += 1

        return self.num_processed / (time.time() - start_time)


@operator('sink')
class Sink(Operator):
    def __init__(self, idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(Sink, self).__init__(idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count,
                                   batch_size, channel_builder)
        self.input = channel_builder.build_input_channel(self.operator_id)
        self.num_done_markers = 0
        self.logger.info('Created sink {}'.format(self.operator_id))

    def run(self):
        done = False
        self.input.connect()
        start_time = time.time()
        while not done:
            timestamp, record_batch = msgpack.unpackb(self.input.get())
            if timestamp > 0:
                self.logger.info('Latency: {}'.format(time.time() - timestamp))
            if record_batch is DONE_MARKER:
                self.num_done_markers += 1
                if self.num_done_markers == self.upstream_count:
                    done = True
                break
            else:
                [self.op_fn(record) for record in record_batch]
                self.num_processed += len(record_batch)
        return self.num_processed / (time.time() - start_time)


class SingleInputOperator(Operator, ABC):
    def __init__(self, idx, operator_id, out_operator_ids, operator_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(SingleInputOperator, self).__init__(idx, operator_id, out_operator_ids, operator_fn, partitioner,
                                                  upstream_count, batch_size, channel_builder)
        self.input = channel_builder.build_input_channel(self.operator_id)
        self.output = [channel_builder.build_output_channel(oid) for oid in out_operator_ids]
        self.output_buffers = OutputBuffers(self.batch_size, self.output)
        self.num_done_markers = 0
        self.state = dict()
        self.logger.info(
            'Created operator {} with downstream operators: {}'.format(self.operator_id, self.out_operator_ids))

    def run(self):
        done = False
        self.input.connect()
        for q in self.output:
            q.connect()
        start_time = time.time()
        while not done:
            timestamp, record_batch = msgpack.unpackb(self.input.get())
            if timestamp > 0:
                self.output_buffers.set_timestamp(timestamp)
            if record_batch is DONE_MARKER:
                self.num_done_markers += 1
                if self.num_done_markers == self.upstream_count:
                    done = True
                    self.output_buffers.close()
            else:
                self.process_batch(record_batch)
                self.num_processed += len(record_batch)

        return self.num_processed / (time.time() - start_time)

    def process_batch(self, record_batch):
        processed_batch = [self.op_fn(record) for record in record_batch]
        self.partitioner.partition_batch(self.output_buffers, processed_batch)

    def num_processed(self):
        return self.num_processed


@operator('map')
class Map(SingleInputOperator):
    def __init__(self, idx, operator_id, out_operator_ids, map_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(Map, self).__init__(idx, operator_id, out_operator_ids, map_fn, partitioner, upstream_count,
                                  batch_size, channel_builder)


@operator('flat_map')
class FlatMap(SingleInputOperator):
    def __init__(self, idx, operator_id, out_operator_ids, flat_map_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(FlatMap, self).__init__(idx, operator_id, out_operator_ids, flat_map_fn, partitioner, upstream_count,
                                      batch_size, channel_builder)

    def process_batch(self, record_batch):
        processed_batch = map(self.op_fn, record_batch)
        for records in processed_batch:
            self.partitioner.partition_batch(self.output_buffers, records)


@operator('filter')
class Filter(SingleInputOperator):
    def __init__(self, idx, operator_id, out_operator_ids, filter_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(Filter, self).__init__(idx, operator_id, out_operator_ids, filter_fn, partitioner, upstream_count,
                                     batch_size, channel_builder)

    def process_batch(self, record_batch):
        processed_batch = filter(self.op_fn, record_batch)
        self.partitioner.partition_batch(self.output_buffers, processed_batch)


@operator('key_by')
class KeyBy(SingleInputOperator):
    def __init__(self, idx, operator_id, out_operator_ids, selector_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(KeyBy, self).__init__(idx, operator_id, out_operator_ids, selector_fn, partitioner, upstream_count,
                                    batch_size, channel_builder)

    def process_record(self, record):
        key = self.op_fn(record)
        self.output_buffers.append(self.partitioner.partition(key), (key, record))

    def process_batch(self, record_batch):
        [self.process_record(record) for record in record_batch]


@operator('reduce')
class Reduce(SingleInputOperator):
    def __init__(self, idx, operator_id, out_operator_ids, reduce_fn, partitioner, upstream_count, batch_size,
                 channel_builder):
        super(Reduce, self).__init__(idx, operator_id, out_operator_ids, reduce_fn, partitioner, upstream_count,
                                     batch_size, channel_builder)

    def process_record(self, record):
        cdef PyObject *obj
        key, value = record

        obj = PyDict_GetItem(self.state, key)
        if obj is NULL:
            new_value = value
        else:
            old_value = <object> obj
            new_value = self.op_fn(old_value, value)
        self.state[key] = new_value
        self.output_buffers.append(self.partitioner.partition(key), (key, new_value))

    def process_batch(self, record_batch):
        [self.process_record(record) for record in record_batch]
