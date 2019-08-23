from lambdastream.channels.channel import DataChannelContext, InputChannel, OutputChannel, channel_context, \
    input_channel, output_channel
from jiffy import JiffyClient


@channel_context('jiffy')
class JiffyChannelContext(DataChannelContext):
    def __init__(self, name, **channel_args):
        super(JiffyChannelContext, self).__init__(name)
        self.host = channel_args.get('jiffy_host', '127.0.0.1')
        self.service_port = channel_args.get('jiffy_service_port', 9090)
        self.lease_port = channel_args.get('jiffy_lease_port', 9091)
        self.path = '/' + self.name
        self.jiffy_client = None

    def init(self):
        self.jiffy_client = JiffyClient(self.host, self.service_port, self.lease_port)
        self.jiffy_client.create_blocking_queue(self.path, 'local://jiffy')

    def destroy(self):
        self.jiffy_client.remove(self.path)


@input_channel('jiffy')
class JiffyInputChannel(InputChannel):
    def __init__(self, name, **channel_args):
        super(JiffyInputChannel, self).__init__(name)
        self.host = channel_args.get('jiffy_host', '127.0.0.1')
        self.service_port = channel_args.get('jiffy_service_port', 9090)
        self.lease_port = channel_args.get('jiffy_lease_port', 9091)
        self.path = '/' + self.name
        self.jiffy_client = None
        self.q = None

    def connect(self):
        self.jiffy_client = JiffyClient(self.host, self.service_port, self.lease_port)
        self.q = self.jiffy_client.open_blocking_queue(self.path)

    def get(self):
        return self.q.get()


@output_channel('jiffy')
class JiffyOutputChannel(OutputChannel):
    def __init__(self, name, **channel_args):
        super().__init__(name)
        self.host = channel_args.get('jiffy_host', '127.0.0.1')
        self.service_port = channel_args.get('jiffy_service_port', 9090)
        self.lease_port = channel_args.get('jiffy_lease_port', 9091)
        self.path = '/' + self.name
        self.jiffy_client = None
        self.q = None

    def connect(self):
        self.jiffy_client = JiffyClient(self.host, self.service_port, self.lease_port)
        self.q = self.jiffy_client.open_blocking_queue(self.path)

    def put(self, data):
        self.q.put(data)

    def flush(self):
        pass
