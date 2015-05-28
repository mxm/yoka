from time import sleep, time
from pprint import pformat

import os, re

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

class ExperimentWithGenerator(Experiment):

    def __init__(self, generator):
        self.generator = generator


class Benchmark(Experiment):

    # timings for each execution's phases
    run_times = {}

    def __init__(self, id, systems, experiment, times=1):
        # unique name of this benchmark
        self.id = id
        self.systems = systems
        self.experiment = experiment
        # number of executions
        self.times = times
        # all timings, set by cluster suite
        self.runs = []
        # clear the dict (if this class gets reused)
        self.run_times.clear()

    @Timer(run_times, "Setup")
    def setup(self):
        for system in self.systems:
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

    # the module for the execution functions and config
    module = None
    # if True, this system is only set up once per suite
    once_per_suite = False
    # skip functions with names in this list
    skip_targets = []
    # path to install directory (set by cluster suite)
    path = None

    def __init__(self, config):
        self.config = config

    def set_config(self):
        self.module.conf = self.config
        self.module.PATH = self.path

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

    # working dir for all files
    working_dir = None

    def __init__(self, config):
        self.config = config

    def setup(self):
        raise NotImplementedError()

    def shutdown(self):
        raise NotImplementedError()

class ClusterSetupException(Exception):
    pass

class ClusterSuite(Experiment):

    run_times = {}

    def __init__(self, id, cluster, systems, generators, benchmarks):
        self.id = id
        # generate unique cluster id
        self.uid = "%s_%d" % (id, int(time()))
        self.cluster = cluster
        self.systems = systems
        self.generators = generators
        self.benchmarks = benchmarks
        # clear the dict (if this class gets reused)
        self.run_times.clear()

    @Timer(run_times, "Setup")
    def setup(self, retry_setup=0):
        # setup cluster
        for run_id in range(1, retry_setup+2):
            is_last_try = run_id == retry_setup+1
            try:
                logger.info("Setting up cluster")
                # first set up cluster
                self.cluster.setup()
                # aggregate all unique systems
                all_systems = set()
                # systems of cluster suite
                for system in self.systems:
                    all_systems.add(system)
                # systems for generators
                for generator in self.generators:
                    for system in generator.systems:
                        all_systems.add(system)
                # systems of benchmarks
                for benchmark in self.benchmarks:
                    for system in benchmark.systems:
                        all_systems.add(system)
                # install & configure unique systems
                for i, system in enumerate(all_systems):
                    # install path
                    system.path = "%s/%s-%d" % (self.cluster.working_dir, system, i)
                    # TODO retry in case of errors (e.g. git clone fails)
                    system.install()
                    system.configure()
                # start systems that run once per cluster suite
                for system in all_systems:
                    if system.once_per_suite:
                        system.start()
                sleep(sleep_time)
            except:
                logger.exception("Failed to set up cluster for the %d/%d time." % (run_id, retry_setup+1))
                if not is_last_try:
                    self.shutdown()
            else:
                break
            if is_last_try:
                raise ClusterSetupException("Failed to set up cluster after %d times." % (retry_setup+1,))

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
            # in case this class got reused
            benchmark.runs = []
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
                for system in benchmark.systems:
                    unique_full_path = "results/logs/%s/%s/%d/%s" % (
                                        self.uid,
                                        benchmark.id,
                                        run_id+1,
                                        system)
                    # create directories
                    os.makedirs(unique_full_path)
                    system.save_log(unique_full_path)
                    log_paths[system] = unique_full_path
                    # check log for exceptions
                    if not failed:
                        try:
                            for filename in os.listdir(unique_full_path):
                                with open(unique_full_path + "/" + filename) as file:
                                    for number, line in enumerate(file):
                                        if re.search("(error|exception)", line, flags=re.IGNORECASE):
                                            logger.info("Error detected in line %d:\n%s" % (number, line))
                                            # for now, just fail the benchmark an error has been detected
                                            failed = True
                                            break
                        except:
                            logger.exception("Failed to scan log for errors.")
                # keep list of results (make copy!)
                benchmark.runs.append(benchmark.run_times.copy())
                # save current result immediately
                result = Result(self, benchmark, log_paths)
                result.save(failed)
                # TODO this could be re-initialized somewhere else
                # CAUTION: run_times holds the same pointer as the decorator Timer
                #          if run_times gets reassigned, this pointer is lost
                benchmark.run_times.clear()
                # raise exception if desired
                if failed and not ignore_failures:
                    raise Exception("Exception raised in %s run %d (see logs)." % (benchmark, run_id))

    @Timer(run_times, "Shutdown")
    def shutdown(self):
        for system in self.systems:
            if system.once_per_suite:
                try:
                    system.stop()
                except:
                    logger.exception("Failed to shutdown system")
        try:
            self.cluster.shutdown()
        except:
            logger.exception("Failed to shutdown cluster")


    def execute(self, retry_setup=0, ignore_benchmark_failures=True,
                shutdown_on_success=True, shutdown_on_failure=True,
                email_results=False):
        # catch all critical exceptions and shutdown server
        run_failure = False
        try:
            try:
                self.setup(retry_setup)
            except:
                logger.exception("Exception trying to setup the cluster for suite %s" % self.id)
                run_failure = True
            else:
                # run generators and benchmarks
                try:
                    # generate data
                    logger.info("Generating data")
                    self.generate()
                except:
                    logger.exception("Exception trying to generate data for suite %s" % self.id)
                    run_failure = True
                else:
                    try:
                        # run benchmarks
                        logger.info("Running benchmarks")
                        self.run(ignore_benchmark_failures)
                    except:
                        logger.exception("Exception trying to run suite %s" % self.id)
                        run_failure = True
        finally:
            # shutdown
            if (not run_failure and shutdown_on_success) or (run_failure and shutdown_on_failure):
                logger.info("Shutting down cluster")
                self.shutdown()
        # TODO this can fail if run_failure is True but not generating any results is bad for debugging
        self.gen_results(email_results)

    def gen_results(self, email_results=False):
        # generate and send results
        filename = None
        report = ""
        try:
            # generate plot for suite UID
            filename = results.gen_plot(self.id)
            # TODO generate plot for suite ID
        except:
            logger.exception("Failed to generate plot")
        try:
            # generate report by printing us using the __str__ method
            report = "%s" % self
            path = "results/logs/%s/report.txt" % self.uid
            # save report to file
            with open(path, "w") as report_file:
                report_file.write(report)
        except:
            logger.exception("Failed to generate report")
        if email_results:
            try:
                if not filename:
                    report += "Plot could not be generated. See log for more details.\n"
                results.send_email(filename, additional_text=report)
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
