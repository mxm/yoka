from time import time

class Experiment(object):

    def setup(self):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()

    def shutdown(self):
        raise NotImplementedError()

    def __str__(self):
        return self.__class__.__name__

class Benchmark(Experiment):

    id = None
    start_time = None
    duration = None

    def __init__(self, id, systems, experiment):
        self.id = id
        self.systems = systems
        self.experiment = experiment

    def setup(self):
        for system in self.systems:
            system.configure()
            system.start()
        self.experiment.setup()

    def run(self):
        self.start_time = time()
        self.experiment.run()
        self.duration = time() - self.start_time

    def shutdown(self):
        self.experiment.shutdown()
        for system in reversed(self.systems):
            system.stop()
            system.reset()

    def __str__(self):
        return str(self.duration)

class System(object):

    def __init__(self, config):
        self.config = config

    def install(self):
        raise NotImplementedError()

    def configure(self):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class Cluster(object):

    def __init__(self, config):
        self.config = config

    def setup(self):
        raise NotImplementedError()

    def shutdown(self):
        raise NotImplementedError()


class ClusterSuite(Experiment):

    def __init__(self, id, cluster, systems, benchmarks):
        self.id = id
        self.cluster = cluster
        self.systems = systems
        self.benchmarks = benchmarks

    def setup(self):
        self.cluster.setup()
        for system in self.systems:
            system.install()

    def run(self):
        for benchmark in self.benchmarks:
            benchmark.setup()
            benchmark.run()
            benchmark.shutdown()

    def shutdown(self):
        self.cluster.shutdown()

    def __str__(self):
        return self.id
