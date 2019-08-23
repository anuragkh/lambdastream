import argparse
import itertools
import logging
import socket
import time

from lambdastream.executors.executor import REGISTERED_EXECUTORS
from lambdastream.wordcount import build_dag

SENTENCE_LENGTH = 100

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


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
        default='redis',
        type=str,
        help='The data channels to use between stream operators'
    )
    parser.add_argument(
        '--sync-host',
        default=socket.gethostname(),
        type=str,
        help='The host address of jiffy directory server'
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
        default=100000,
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
    dag, contexts = build_dag(**vars(args))

    word_count = REGISTERED_EXECUTORS[args.executor](**vars(args))
    result_prefix = args.channel + '_batch' + str(args.batch_size) + '_mapper' + str(
        args.num_mappers) + '_reducer' + str(args.num_reducers)

    for ctx in contexts:
        ctx.init()

    try:
        start = time.time()
        res = word_count.exec(dag)
        if res is not None:
            throughputs, latencies = res
            # Compute source throughputs
            source_throughputs = [throughputs[key] for key in throughputs.keys() if key.startswith('source')]
            # Compute latencies
            sink_latencies = latencies.values()

            def flatten(l):
                return [item for sublist in l for item in sublist]

            flattened = flatten(sink_latencies)
            avg_latencies = [sum(l) / len(l) for l in sink_latencies]
            print('THROUGHPUT:: Total: {}, Breakdown: {}'.format(sum(source_throughputs), source_throughputs))
            print('LANTENCY:: Total Avg.: {}, Breakdown: {}'.format(sum(avg_latencies) / len(avg_latencies),
                                                                    avg_latencies))
            with open(result_prefix + '_throughput.txt', 'w') as out:
                for t in source_throughputs:
                    out.write('{}\n'.format(t))

            with open(result_prefix + '_latency.txt', 'w') as out:
                for l in flattened:
                    out.write('{}\n'.format(l))
        else:
            total = time.time() - start
            throughput = float(args.num_records) / total
            print('Throughput: {} records/s'.format(throughput))
    finally:
        for ctx in contexts:
            ctx.destroy()


if __name__ == '__main__':
    main()
