from jiffy.storage.block_client import BlockClientCache
from jiffy.storage.compat import b, bytes_to_str
from jiffy.storage.replica_chain_client import ReplicaChainClient


class DataStructureClient(object):
    def __init__(self, fs, path, block_info, op_types, timeout_ms=1000):
        self.redo_times = 0
        self.fs = fs
        self.path = path
        self.client_cache = BlockClientCache(timeout_ms)
        self.block_info = block_info
        self.op_types = op_types
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain, self.op_types) for chain
                       in self.block_info.data_blocks]

    def _refresh(self):
        self.block_info = self.fs.dstatus(self.path)
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain, self.op_types) for chain
                       in self.block_info.data_blocks]

    def _handle_redirect(self, args, response):
        raise NotImplementedError()

    def _block_id(self, args):
        raise NotImplementedError()

    def _run_repeated(self, args):
        response = None
        while response is None:
            response = self.blocks[self._block_id(args)].run_command(args)
            response = self._handle_redirect(args, response)
        self.redo_times = 0
        if response[0] != b('!ok'):
            raise KeyError(bytes_to_str(response[0]))
        return response
