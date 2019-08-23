from abc import ABC, abstractmethod

from operator.operator import REGISTERED_OPERATORS


class StageBuilder(ABC):
    def __init__(self, name, stage_idx, parallelism, channel_builder):
        self.name = name
        self.stage_idx = stage_idx
        self.parallelism = parallelism
        self.channel_builder = channel_builder

    def operator_ids(self):
        return ['{}_stage_{}_idx_{}'.format(self.name, self.stage_idx, op_idx) for op_idx in range(self.parallelism)]

    @abstractmethod
    def build_stage(self, output_operator_ids, upstream_count):
        pass


class RegisteredStageBuilder(StageBuilder):
    def __init__(self, name, stage_idx, parallelism, channel_builder, op_fn, batch_size, partitioner_cls):
        super().__init__(name, stage_idx, parallelism, channel_builder)
        self.op_fn = op_fn
        self.batch_size = batch_size
        self.partitioner_cls = partitioner_cls

    def build_stage(self, output_operator_ids, upstream_count):
        op_ids = self.operator_ids()
        partitioner = self.partitioner_cls(len(output_operator_ids))
        return [REGISTERED_OPERATORS[self.name](idx, op_ids[idx], output_operator_ids, self.op_fn, partitioner,
                                                upstream_count, self.batch_size, self.channel_builder) for idx in
                range(self.parallelism)]


class CustomStageBuilder(StageBuilder):
    def __init__(self, name, stage_idx, parallelism, channel_builder, operator_cls, **kwargs):
        super().__init__(name, stage_idx, parallelism, channel_builder)
        self.operator_cls = operator_cls
        self.kwargs = kwargs

    def build_stage(self, output_operator_ids, upstream_count):
        op_ids = self.operator_ids()
        return [self.operator_cls(idx, op_ids[idx], output_operator_ids, upstream_count, self.channel_builder,
                                  **self.kwargs) for idx in
                range(self.parallelism)]


class DAGBuilder(object):
    def __init__(self, channel_builder):
        self.stages = []
        self.stage_idx = 0
        self.channel_builder = channel_builder

    def add_stage(self, op_name, parallelism, op_fn, batch_size, partitioner_cls):
        self.stages.append(
            RegisteredStageBuilder(op_name, self.stage_idx, parallelism, self.channel_builder, op_fn, batch_size,
                                   partitioner_cls))
        self.stage_idx += 1

    def add_custom_stage(self, op_name, parallelism, operator_cls, **kwargs):
        self.stages.append(
            CustomStageBuilder(op_name, self.stage_idx, parallelism, self.channel_builder, operator_cls, **kwargs))
        self.stage_idx += 1

    def get_stage(self, stage):
        return self.stages[stage]

    def build(self):
        dag = []
        channels = []
        out_op_ids = []
        for stage_idx in range(len(self.stages) - 1, -1, -1):
            op_ids = self.stages[stage_idx].operator_ids()
            channels.extend(op_ids)
            upstream_count = len(self.stages[stage_idx - 1].operator_ids()) if stage_idx > 0 else 0
            dag.append(self.stages[stage_idx].build_stage(out_op_ids, upstream_count))
            out_op_ids = op_ids
        self.stages.clear()
        self.stage_idx = 0
        return dag, [self.channel_builder.build_channel_ctx(channel) for channel in channels]
