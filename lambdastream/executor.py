import logging
from abc import ABC, abstractmethod

from lambdastream.config import LOG_LEVEL

REGISTERED_EXECUTORS = dict()

logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")


def executor(name):
    def register(executor_cls):
        logging.info('Registered executor class {} for {}'.format(executor_cls, name))
        REGISTERED_EXECUTORS[name] = executor_cls
        return executor_cls

    return register


class Executor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def exec(self, dag):
        pass
