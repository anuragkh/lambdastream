from lambdastream.operator import REGISTERED_OPERATORS


class Stage(object):
    def __init__(self, op_type, op_fn, stage_idx, parallelism, partitioner_cls):
        self.op_type = op_type
        self.op_fn = op_fn
        self.stage_idx = stage_idx
        self.parallelism = parallelism
        self.partitioner_cls = partitioner_cls

    def operator_ids(self):
        return ['{}_stage_{}_idx_{}'.format(self.op_type, self.stage_idx, op_idx) for op_idx in range(self.parallelism)]


class DAGBuilder(object):
    def __init__(self, batch_size, channel_builder):
        self.stages = []
        self.stage_idx = 0
        self.batch_size = batch_size
        self.channel_builder = channel_builder

    def add_stage(self, op_type, op_fn, parallelism, partitioner_cls):
        self.stages.append(Stage(op_type, op_fn, self.stage_idx, parallelism, partitioner_cls))
        self.stage_idx += 1

    def get_stage(self, stage):
        return self.stages[stage]

    def build(self):
        assert self.stages[0].op_type == 'source', 'DAG must start with source node'
        assert self.stages[-1].op_type == 'sink', 'DAG must end with sink node'
        dag = []
        channels = []
        out_op_ids = []
        for stage_idx in range(len(self.stages) - 1, -1, -1):
            op_type = self.stages[stage_idx].op_type
            op_ids = self.stages[stage_idx].operator_ids()
            channels.extend(op_ids)
            partitioner = self.stages[stage_idx].partitioner_cls(len(out_op_ids))
            op_fn = self.stages[stage_idx].op_fn
            upstream_count = len(self.stages[stage_idx - 1].operator_ids()) if stage_idx > 0 else 0
            dag.append(
                [REGISTERED_OPERATORS[op_type](idx, op_ids[idx], out_op_ids, op_fn, partitioner, upstream_count,
                                               self.batch_size, self.channel_builder)
                 for idx in
                 range(self.stages[stage_idx].parallelism)])
            out_op_ids = op_ids
        self.stages.clear()
        self.stage_idx = 0
        return dag, [self.channel_builder.build_channel_ctx(channel) for channel in channels]
