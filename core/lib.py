from time import sleep, time
from pprint import pformat

import datetime

from utils import Timer

import results

import log

logger = log.get_logger(__name__)

from results import Result

sleep_time = 10

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
    # timings for run phases
    run_times = {}
    # all timings, set by cluster suite
    runs = []
    
    def __init__(self, id, systems, experiment, times = 1):
        self.id = id
        self.systems = systems
        self.experiment = experiment
        self.times = times

    @Timer(run_times, "Setup")
    def setup(self):
        for system in self.systems:
            system.set_config()
            # if benchmark has not run yet, configure
            if not self.runs:
                system.configure()
            system.start()
            sleep(sleep_time)
        self.experiment.setup()

    @Timer(run_times, "Benchmark")
    def run(self):
        self.experiment.run()

    @Timer(run_times, "Shutdown")
    def shutdown(self):
        try:
            self.experiment.shutdown()
        except:
            logger.exception("Shutting down benchmark %s failed." % self.id)
        for system in reversed(self.systems):
            system.stop()
            system.reset()

    def __str__(self):
        s = "%s:\n\n" % self.id
        for system in self.systems:
            s += "%s config:\n%s\n\n" % (system, pformat(system.config))
        s += "\n"
        for run_time in self.runs:
            s += Timer.format_run_times(run_time)
            s += "\n"
        s += "\n"
        return s

# reuse the same logic the Benchmark class
class Generator(Benchmark):
    pass

class System(object):

    module = None
    once_per_suite = False
    skip_targets = []

    def __init__(self, config):
        self.config = config

    def set_config(self):
        self.module.conf = self.config

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

    def save_log(self, destination):
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

    run_times = {}

    def __init__(self, id, cluster, systems, generators, benchmarks):
        self.id = id
        self.uid = "%s_%d" % (id, int(time()))
        self.cluster = cluster
        self.systems = systems
        self.generators = generators
        self.benchmarks = benchmarks

    @Timer(run_times, "Setup")
    def setup(self):
        self.cluster.setup()
        for system in self.systems:
            system.install()
            if system.once_per_suite:
                system.configure()
                system.start()
                sleep(sleep_time)

    @Timer(run_times, "Data generation")
    def generate(self):
        # generate data
        for generator in self.generators:
            generator.setup()
            generator.run()
            generator.shutdown()

    @Timer(run_times, "Benchmark")
    def run(self, ignore_failures=False):
        # execute benchmarks
        for benchmark in self.benchmarks:
            for run_id in range(0, benchmark.times):
                failed = False
                try:
                    benchmark.setup()
                    benchmark.run()
                except:
                    logger.exception("Exception in %s run %d" % (benchmark, run_id))
                    failed = True
                finally:
                    try:
                        benchmark.shutdown()
                    except:
                        pass
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
                benchmark.runs.append(benchmark.run_times)
                result = Result(self, benchmark, log_paths)
                result.save(failed)
                if failed and not ignore_failures:
                    raise Exception("Exception raised in %s run %d (see logs)." % (benchmark, run_id))

    @Timer(run_times, "Shutdown")
    def shutdown(self):
        for system in self.systems:
            if system.once_per_suite:
                system.stop()
        self.cluster.shutdown()


    def execute(self, retry_setup=0, ignore_failures=True,
                shutdown_on_success=True, shutdown_on_failure=True,
                email_results=False):
        # setup cluster
        for run_id in range(1, retry_setup+2):
            try:
                logger.info("Setting up cluster")
                self.setup()
                setup_failure = False
                break
            except:
                logger.exception("Failed to set up cluster for the %d/%d time." % (run_id, retry_setup+1))
                setup_failure = True
        if not setup_failure:
            try:
                # generate data
                self.generate()
                # run benchmarks
                logger.info("Running benchmarks")
                self.run(ignore_failures)
                run_failure = False
            except:
                logger.exception("Exception trying to run suite %s" % self.id)
                run_failure = True
            finally:
                # shutdown
                if (not run_failure and shutdown_on_success) or (run_failure and shutdown_on_failure):
                    logger.info("Shutting down cluster")
                    self.shutdown()
            if email_results and not run_failure:
                try:
                    filename = results.gen_plot(self.id)
                    text = "%s" % self
                    if not filename:
                        text += "Plot could not be generated. See log for more details.\n"
                    results.send_email(filename, additional_text=text)
                except:
                    logger.exception("Failed to send results.")

    def __str__(self):
        # print run times
        s = "Cluster suite %s\n\n" % self.id
        s += Timer.format_run_times(self.run_times)
        s += "\n"
        # print system configs
        s += "Cluster %s config\n%s\n\n" % (self.cluster.__class__.__name__, pformat(self.cluster.config))
        for system in self.systems:
            s += "%s config:\n%s\n\n" % (system, pformat(system.config))
        s += "\n"
        # print benchmark times and system configs
        for benchmark in self.benchmarks:
            s += "%s\n\n" % benchmark
        return s
