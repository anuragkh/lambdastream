import queue

from lambdastream.channels.channel import DataChannelContext, InputChannel, OutputChannel, output_channel, \
    input_channel, channel_context


@channel_context('local')
class LocalChannelContext(DataChannelContext):
    def __init__(self, name, **channel_args):
        super().__init__(name)
        self.queue_dict = channel_args['queue_dict']
        self.max_size = channel_args['max_queue_length']

    def init(self):
        self.queue_dict[self.name] = queue.Queue(self.max_size)

    def destroy(self):
        del self.queue_dict[self.name]


@input_channel('local')
class LocalInputChannel(InputChannel):
    def __init__(self, name, **channel_args):
        super(LocalInputChannel, self).__init__(name)
        self.queue_dict = channel_args['queue_dict']
        self.q = None

    def connect(self):
        self.q = self.queue_dict[self.name]

    def get(self):
        return self.q.get()


@output_channel('local')
class LocalOutputChannel(OutputChannel):
    def __init__(self, name, **channel_args):
        super(LocalOutputChannel, self).__init__(name)
        self.queue_dict = channel_args['queue_dict']
        self.q = None

    def connect(self):
        self.q = self.queue_dict[self.name]

    def put(self, record):
        self.q.put(record)

    def flush(self):
        pass
