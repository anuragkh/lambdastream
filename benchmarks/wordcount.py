import argparse
import logging
import time

import numpy as np

from lambdastream.constants import DONE_MARKER
from lambdastream.context import StreamContext
from lambdastream.operator import HashPartitioner

SENTENCE_LENGTH = 100

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


class WordSource(object):
    def __init__(self, words_file, num_records, timestamp_interval):
        self.num_records = num_records
        self.generated = 0
        self.timestamp_interval = timestamp_interval
        with open(words_file) as f:
            self.words = []
            for line in f.readlines():
                self.words.append(line.strip())
        self.words = np.array([word.encode('ascii') for word in self.words])
        np.random.seed(0)

    def __call__(self, batch_size):
        if self.generated >= self.num_records:
            return DONE_MARKER
        sentences = [np.string_.join(b' ', np.random.choice(self.words, SENTENCE_LENGTH)) for _ in range(batch_size)]
        self.generated += batch_size
        return sentences


class MeasureLatency(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def __call__(self, record):
        word, count_ts = record
        count, timestamp = count_ts
        if timestamp > 0:
            latency = time.time() - timestamp
            self.logger.info('Latency: {}'.format(latency))


def main():
    parser = argparse.ArgumentParser(description='Word count benchmark.')
    parser.add_argument(
        '--executor',
        default='local',
        type=str,
        help='The executor to use for stream operators'
    )
    parser.add_argument(
        '--channel',
        default='local',
        type=str,
        help='The data channels to use between stream operators'
    )
    parser.add_argument(
        '--jiffy-host',
        default='127.0.0.1',
        type=str,
        help='The host address of jiffy directory server'
    )
    parser.add_argument(
        '--jiffy-service-port',
        default=9090,
        type=int,
        help='The service port for jiffy directory server'
    )
    parser.add_argument(
        '--jiffy-lease-port',
        default=9091,
        type=int,
        help='The lease port for jiffy directory server'
    )
    parser.add_argument(
        '--redis-host',
        default='127.0.0.1',
        type=str,
        help='The host address of redis server'
    )
    parser.add_argument(
        '--redis-port',
        default=6379,
        type=int,
        help='The port for redis server'
    )
    parser.add_argument(
        '--redis-db',
        default=0,
        type=int,
        help='The db number for redis server'
    )
    parser.add_argument(
        '--num-mappers',
        default=1,
        type=int,
        help='The number of mappers to use.')
    parser.add_argument(
        '--num-reducers',
        default=1,
        type=int,
        help='The number of reducers to use.')
    parser.add_argument(
        '--words-file',
        type=str,
        required=True,
        help='Words file')
    parser.add_argument(
        '--batch-size',
        type=int,
        default=64,
        help='Batch size')
    parser.add_argument(
        '--num-records',
        type=int,
        default=10000,
        help='Number of records to generate')
    parser.add_argument(
        '--max-queue-length',
        type=int,
        default=8,
        help='Queue length')
    parser.add_argument(
        '--latency-file',
        type=str,
        default='latency.txt',
        help='')
    parser.add_argument(
        '--timestamp-interval',
        type=int,
        default=1000,
        help='Each source will output a timestamp after this many records')

    args = parser.parse_args()
    source = WordSource(args.words_file, args.num_records, args.timestamp_interval)
    ctx = StreamContext(**vars(args))
    word_count = ctx.create_stream('wordcount', source, args.num_mappers) \
        .flat_map(lambda x: [(w, 1) for w in x.split()], args.num_mappers, HashPartitioner) \
        .reduce_by_key(lambda a, b: a + b, args.num_reducers)

    start = time.time()
    word_count.run()
    total = time.time() - start

    throughput = float(args.num_records) / total
    print('Throughput: {} records/s'.format(throughput))


if __name__ == '__main__':
    main()
