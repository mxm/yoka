from __future__ import with_statement
from fabric.api import execute

from cluster import gcloud, maintenance, hadoop, flink

import pkgutil

import benchmarks

class Experiment(object):

    def setup(self):
        raise NotImplementedError()

    def run(self, job):
        raise NotImplementedError()

    def shutdown(self):
        raise NotImplementedError()

    def __str__(self):
        return self.__class__.__name__


class Suite(Experiment):

    def __init__(self, experiments):
        self.experiments = experiments

    def run(self):
        for experiment in self.experiments:
            print "running", experiment
            experiment.setup()
            experiment.run()
            experiment.shutdown()


class FlinkBenchmark(Experiment):

    def setup(self):
         execute(flink.master)
         execute(flink.slaves)

    def run(self):
        raise NotImplementedError()

    def shutdown(self):
        execute(flink.slaves, "stop")
        execute(flink.master, "stop")

class FlinkBenchmarkSuite(Suite):

    def __init__(self, *args, **kwargs):
        super(FlinkBenchmarkSuite, self).__init__(*args, **kwargs)

    def setup(self):
        [execute(step) for step in [
            gcloud.create_instances,
            maintenance.update_package_cache,
            maintenance.install_dependencies,
            maintenance.set_java_home,
            maintenance.set_key,
            hadoop.install,
            hadoop.configure,
            hadoop.pull,
            hadoop.master,
            hadoop.slaves,
            flink.install,
            flink.configure,
            flink.pull]
        ]

    def shutdown(self):
        execute(hadoop.slaves, "stop")
        execute(hadoop.master, "stop")
        execute(gcloud.delete_instances)


if __name__ == '__main__':
    for importer, modname, ispkg in pkgutil.iter_modules(benchmarks.__path__):
        if not ispkg:
            print "running %s" % modname
            module = __import__("benchmarks.%s" % modname, globals(), locals(), ["SUITE"], -1)
            suite = module.SUITE
            suite.setup()
            suite.run()
            suite.shutdown()
