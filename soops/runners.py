from dask.distributed import Client, LocalCluster

from concurrent.futures import ThreadPoolExecutor

from soops.base import load_classes, Struct

class Runner(Struct):

    def any_from_options(options):
        files = ['runners.py']
        table = load_classes(files, [Runner], package_name='soops')
        return table[options.runner](options.n_workers, options.cluster_kwargs)

class DaskRunner(Runner):
    name = 'dask'

    def __init__(self, n_workers, runner_kwargs):
        from dask.distributed import as_completed
        Runner.__init__(self, n_workers=n_workers, runner_kwargs=runner_kwargs,
                        as_completed=as_completed)

        self.cluster = LocalCluster(n_workers=n_workers, **runner_kwargs)
        self.client = Client(self.cluster)

    def submit(self, *args, **kwargs):
        call = self.client.submit(*args, pure=False, **kwargs)
        return call

    def close(self):
        self.client.close()
        self.cluster.close()

class ThreadPoolRunner(Runner):
    name = 'threadpool'

    def __init__(self, n_workers, runner_kwargs):
        from concurrent.futures import as_completed
        Runner.__init__(self, n_workers=n_workers, runner_kwargs=runner_kwargs,
                        as_completed=as_completed)

        self.executor = ThreadPoolExecutor(max_workers=n_workers,
                                           **runner_kwargs)

    def submit(self, *args, **kwargs):
        call = self.executor.submit(*args, **kwargs)
        return call

    def close(self):
        self.executor.shutdown()
