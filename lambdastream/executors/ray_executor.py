import ray
from ray.tests.cluster_utils import Cluster

from lambdastream.executor import Executor, executor


@ray.remote
class OperatorActor(object):
    def __init__(self, op):
        self.operator = op

    def run(self):
        self.operator.run()


@executor('ray')
class RayExecutor(Executor):
    def __init__(self, **kwargs):
        super(RayExecutor, self).__init__()
        mode = kwargs.get('ray_mode', 'local')
        self.resource_idx = 0
        if mode == 'local':
            node_kwargs = {
                'num_cpus': 4,
                'object_store_memory': 10 ** 9,
                'resources': {
                    'Node_0': 100
                }
            }
            self.cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
            self.num_nodes = kwargs.get('ray_num_nodes', 4)
            self.nodes = []
            self.resources = []
            i = 1
            for _ in range(self.num_nodes):
                node, resource = self._create_local_node(i, node_kwargs)
                self.nodes.append(node)
                self.resources.append(resource)
            self._create_local_node(i, node_kwargs)
            redis_address = self.cluster.redis_address
            ray.init(redis_address=redis_address)
        else:
            redis_address = kwargs.get('redis_address', '127.0.0.1')
            ray.init(redis_address=redis_address)

            self.resources = []
            self.nodes = ray.global_state.client_table()
            for node in self.nodes:
                for resource in node['Resources']:
                    if 'Node' in resource and resource != 'Node_0':
                        self.resources.append(resource)

    def __del__(self):
        self.cluster.shutdown()
        ray.disconnect()

    def _create_local_node(self, i, node_kwargs):
        resource = 'Node_{}'.format(i)
        node_kwargs['resources'] = {resource: 100}
        node = self.cluster.add_node(**node_kwargs)
        return node, resource

    def get_next_resource(self):
        resource = self.resources[self.resource_idx % self.num_nodes]
        self.resource_idx += 1
        return resource

    def exec(self, dag):
        num_stages = len(dag)
        actors = []
        task_handles = []
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                actor = OperatorActor._remote(args=[operator], kwargs={}, resources={self.get_next_resource(): 1})
                actors.append(actor)
                task_handles.append(actor.run.remote())

        ray.get(task_handles)
