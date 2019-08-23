from abc import ABC, abstractmethod


class State(ABC):
    def __init__(self, *args):
        self.args = args

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def cleanup(self):
        pass
