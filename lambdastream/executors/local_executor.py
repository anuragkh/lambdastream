from threading import Thread

from lambdastream.executor import Executor, executor


@executor('local')
class LocalExecutor(Executor):
    def __init__(self):
        super(LocalExecutor, self).__init__()

    def exec(self, dag):
        threads = []
        num_stages = len(dag)
        for i in range(num_stages):
            stage = dag.pop()
            for operator in stage:
                t = Thread(target=operator.run)
                threads.append(t)
                t.start()

        for t in threads:
            t.join()
