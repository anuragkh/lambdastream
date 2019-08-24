import argparse
import logging
import pathlib
import socket
import time

from lambdastream.channels.channel import REGISTERED_CHANNELS, ChannelBuilder
from lambdastream.executors.executor import REGISTERED_EXECUTORS
from lambdastream.wordcount import Sink, Reducer, Mapper, WordSource

SENTENCE_LENGTH = 100

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def build_dag(**kwargs):
    channel = kwargs.get('channel', 'redis')
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

    # One source per mapper.
    source_keys = ['source_{}'.format(i) for i in range(num_mappers)]
    mapper_keys = ['mapper_{}'.format(i) for i in range(num_mappers)]
    reducer_keys = ['reducer_{}'.format(i) for i in range(num_reducers)]
    sink_keys = ['sink_{}'.format(i) for i in range(num_reducers)]
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


def flatten(l):
    return [item for sublist in l for item in sublist]


def process_results(res, result_prefix):
    throughputs, latencies, read_latencies, write_latencies = res

    # compute throughputs
    source_throughputs = [throughputs[key] for key in throughputs.keys() if key.startswith('source')]

    # Compute latencies
    sink_latencies = latencies.values()
    all_sink_latencies = flatten(sink_latencies)

    # Compute average operator read/write latencies
    avg_read_latencies = {op: sum(values) for op, values in read_latencies.items()}
    avg_write_latencies = {op: sum(values) for op, values in write_latencies.items()}

    avg_latencies = [sum(l) / len(l) for l in sink_latencies]
    print('THROUGHPUT:: Total: {}, Breakdown: {}'.format(sum(source_throughputs), source_throughputs))
    print('LATENCY:: Total Avg.: {}, Breakdown: {}'.format(sum(avg_latencies) / len(avg_latencies), avg_latencies))
    print('READ LATENCY:: Avg. Breakdown: {}'.format(avg_read_latencies))
    print('WRITE LATENCY:: Avg. Breakdown: {}'.format(avg_write_latencies))
    with open(result_prefix + '_throughput.txt', 'w') as out:
        for t in source_throughputs:
            out.write('{}\n'.format(t))

    with open(result_prefix + '_latency.txt', 'w') as out:
        for l in all_sink_latencies:
            out.write('{}\n'.format(l))

    for key in read_latencies.keys():
        with open(result_prefix + '_' + key + '_read_latency.txt', 'w') as out:
            for t in read_latencies[key]:
                out.write('{}\n'.format(t))

    for key in write_latencies.keys():
        with open(result_prefix + '_' + key + '_write_latency.txt', 'w') as out:
            for t in write_latencies[key]:
                out.write('{}\n'.format(t))


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
    pathlib.Path('results').mkdir(parents=True, exist_ok=True)
    pathlib.Path('results/' + args.channel).mkdir(parents=True, exist_ok=True)
    pathlib.Path('results/' + args.channel + '/' + args.executor).mkdir(parents=True, exist_ok=True)
    result_prefix = 'results/' + args.channel + '/' + args.executor + '/batch' + str(args.batch_size) + '_mapper' + str(
        args.num_mappers) + '_reducer' + str(args.num_reducers)

    for ctx in contexts:
        ctx.init()

    try:
        start = time.time()
        res = word_count.exec(dag)
        if res is not None:
            process_results(res, result_prefix)
        else:
            total = time.time() - start
            throughput = float(args.num_records) / total
            print('Throughput: {} records/s'.format(throughput))
    finally:
        for ctx in contexts:
            ctx.destroy()


if __name__ == '__main__':
    main()
