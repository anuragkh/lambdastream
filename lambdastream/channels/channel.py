import logging
from abc import abstractmethod, ABC

from lambdastream.config import LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")

REGISTERED_CHANNELS = dict()


def input_channel(channel_name):
    def register(channel_cls):
        logging.info('Registered input channels class {} for {}'.format(channel_cls, channel_name))
        REGISTERED_CHANNELS.setdefault(channel_name, dict())['input_channel'] = channel_cls
        return channel_cls

    return register


def output_channel(channel_name):
    def register(channel_cls):
        logging.info('Registered output channels class {} for {}'.format(channel_cls, channel_name))
        REGISTERED_CHANNELS.setdefault(channel_name, dict())['output_channel'] = channel_cls
        return channel_cls

    return register


def channel_context(channel_name):
    def register(channel_cls):
        logging.info('Registered channels context class {} for {}'.format(channel_cls, channel_name))
        REGISTERED_CHANNELS.setdefault(channel_name, dict())['channel_context'] = channel_cls
        return channel_cls

    return register


class ChannelBuilder(object):
    def __init__(self, channel_ctx_cls, input_channel_cls, output_channel_cls, **channel_args):
        self.channel_ctx_cls = channel_ctx_cls
        self.input_channel_cls = input_channel_cls
        self.output_channel_cls = output_channel_cls
        self.channel_args = channel_args

    def build_channel_ctx(self, name):
        return self.channel_ctx_cls(name, **self.channel_args)

    def build_input_channel(self, name):
        return self.input_channel_cls(name, **self.channel_args)

    def build_output_channel(self, name):
        return self.output_channel_cls(name, **self.channel_args)


class DataChannel(ABC):
    def __init__(self, name, **channel_args):
        self.name = name

    @abstractmethod
    def connect(self):
        pass


class DataChannelContext(ABC):
    def __init__(self, name, **channel_args):
        self.name = name

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def destroy(self):
        pass


class InputChannel(DataChannel, ABC):
    def __init__(self, name, **channel_args):
        super(InputChannel, self).__init__(name, **channel_args)

    @abstractmethod
    def get(self):
        pass


class OutputChannel(DataChannel, ABC):
    def __init__(self, name, **channel_args):
        super(OutputChannel, self).__init__(name, **channel_args)

    @abstractmethod
    def put(self, record):
        pass

    @abstractmethod
    def flush(self):
        pass
