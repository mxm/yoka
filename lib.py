from time import time

from results import Result

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
    executed = 0

    def __init__(self, id, systems, experiment, times = 1):
        self.id = id
        self.systems = systems
        self.experiment = experiment
        self.times = times

    def setup(self):
        for system in self.systems:
            if not self.executed:
                system.configure()
            system.start()
        self.experiment.setup()

    def run(self):
        self.start_time = time()
        self.experiment.run()
        self.duration = time() - self.start_time
        self.executed += 1

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

    def save_log(self):
        raise NotImplementedError()

    def __str__(self):
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
        self.uid = "%s_%d" % (id, int(time()))
        self.cluster = cluster
        self.systems = systems
        self.benchmarks = benchmarks

    def setup(self):
        self.cluster.setup()
        for system in self.systems:
            system.install()

    def run(self):
        for benchmark in self.benchmarks:
            for run_id in range(0, benchmark.times):
                benchmark.setup()
                benchmark.run()
                benchmark.shutdown()
                # get system logs
                log_paths = {}
                for system in self.systems:
                    unique_full_path = "logs/%s/%s/%d/%s" % (self.uid,
                                                             benchmark.id,
                                                             run_id+1,
                                                             system)
                    system.save_log(unique_full_path)
                    log_paths[system] = unique_full_path
                # save result
                result = Result(self, benchmark, log_paths)
                result.save()



    def shutdown(self):
        self.cluster.shutdown()

    def __str__(self):
        return self.id
