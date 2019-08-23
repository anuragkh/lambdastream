from multiprocessing import Process

from lambdastream.executors.executor import Executor, executor


@executor('process')
class ProcessExecutor(Executor):
    def __init__(self, **kwargs):
        super(ProcessExecutor, self).__init__(**kwargs)

    def exec(self, dag):
        processes = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                p = Process(target=operator.run)
                processes.append(p)
                p.start()

        for p in processes:
            p.join()
