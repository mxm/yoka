from core.lib import System

from cluster import hadoop, flink, tez, spark
from fabric.api import execute

class Hadoop(System):
    """
    Installs, configures, and starts
    Hadoop v2 including HDFS, YARN, and MapReduce
    """

    module = hadoop
    once_per_suite = True

    def __init__(self, config):
        self.config = config

    def install(self):
        if 'install' not in self.skip_targets:
            execute(hadoop.install)

    def configure(self):
        if 'configure' not in self.skip_targets:
            execute(hadoop.configure)
        execute(hadoop.pull)

    def reset(self):
        #execute(hadoop.delete_data_slaves)
        pass

    def start(self):
        if 'start' not in self.skip_targets:
            execute(hadoop.master)
            execute(hadoop.slaves)

    def stop(self):
        execute(hadoop.slaves, 'stop')
        execute(hadoop.master, 'stop')

    def save_log(self, log_name):
        # TODO
        pass

    def __str__(self):
        return "hadoop"


class Tez(System):

    module = tez
    once_per_suite = True

    def __init__(self, config):
        self.config = config

    def install(self):
        if 'install' not in self.skip_targets:
            execute(tez.install)

    def configure(self):
        if 'configure' not in self.skip_targets:
            execute(tez.configure)

    def reset(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def save_log(self, log_name):
        pass

    def __str__(self):
        return "tez"


class Flink(System):

    module = flink
    once_per_suite = False

    def __init__(self, config):
        self.config = config

    def install(self):
        if 'install' not in self.skip_targets:
            execute(flink.install)

    def configure(self):
        if 'configure' not in self.skip_targets:
            execute(flink.configure)
        execute(flink.pull)

    def reset(self):
        pass

    def start(self):
        execute(flink.master)
        execute(flink.slaves)

    def stop(self):
        execute(flink.slaves, 'stop')
        execute(flink.master, 'stop')

    def save_log(self, unique_full_path):
        execute(flink.copy_log_master, unique_full_path)
        execute(flink.copy_log_slaves, unique_full_path)

    def __str__(self):
        return "flink"


class Spark(System):

    module = spark
    once_per_suite = False

    def __init__(self, config):
        self.config = config

    def install(self):
        if 'install' not in self.skip_targets:
            execute(spark.install)

    def configure(self):
        if 'configure' not in self.skip_targets:
            execute(spark.configure)
        execute(spark.pull)

    def reset(self):
        pass

    def start(self):
        execute(spark.master)
        execute(spark.slaves)

    def stop(self):
        execute(spark.slaves, 'stop')
        execute(spark.master, 'stop')

    def save_log(self, unique_full_path):
        # TODO
        pass

    def __str__(self):
        return "spark"

