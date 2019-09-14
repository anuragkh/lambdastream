import redis

from lambdastream.channels.channel import DataChannelContext, InputChannel, OutputChannel, output_channel, \
    input_channel, channel_context


@channel_context('redis')
class RedisChannelContext(DataChannelContext):
    def __init__(self, name, **channel_args):
        super().__init__(name)
        redis_eps = channel_args.get('redis_host', '127.0.0.1:6379:0').split(',')
        ep = redis_eps[hash(name) % len(redis_eps)].split(':')
        print('Hashed to redis host: {}'.format(ep))
        self.host = ep[0]
        self.port = int(ep[1])
        self.db = int(ep[2])
        self.conn = None

    def init(self):
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db)

    def destroy(self):
        self.conn.delete(self.name)


@input_channel('redis')
class RedisInputChannel(InputChannel):
    def __init__(self, name, **channel_args):
        super(RedisInputChannel, self).__init__(name)
        redis_eps = channel_args.get('redis_host', '127.0.0.1:6379:0').split(',')
        ep = redis_eps[hash(name) % len(redis_eps)].split(':')
        self.host = ep[0]
        self.port = int(ep[1])
        self.db = int(ep[2])
        self.conn = None

    def connect(self):
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db)

    def get(self):
        return self.conn.blpop(self.name)[1]


@output_channel('redis')
class RedisOutputChannel(OutputChannel):
    def __init__(self, name, **channel_args):
        super().__init__(name)
        redis_eps = channel_args.get('redis_host', '127.0.0.1:6379:0').split(',')
        ep = redis_eps[hash(name) % len(redis_eps)].split(':')
        self.host = ep[0]
        self.port = int(ep[1])
        self.db = int(ep[2])
        self.conn = None

    def connect(self):
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db)

    def put(self, data):
        self.conn.rpush(self.name, data)

    def flush(self):
        pass
