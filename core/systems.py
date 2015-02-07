from core.lib import System

from cluster import hadoop, flink, tez
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
        self.set_config()
        if 'install' not in self.skip_targets:
            execute(hadoop.install)

    def configure(self):
        self.set_config()
        if 'configure' not in self.skip_targets:
            execute(hadoop.configure)
        execute(hadoop.pull)
        execute(hadoop.set_environment_variables)

    def reset(self):
        self.set_config()
        execute(hadoop.delete_data_slaves)

    def start(self):
        self.set_config()
        if 'start' not in self.skip_targets:
            execute(hadoop.master)
            execute(hadoop.slaves)

    def stop(self):
        self.set_config()
        execute(hadoop.slaves, 'stop')
        execute(hadoop.master, 'stop')

    def save_log(self, log_name):
        pass

    def __str__(self):
        return "hadoop"


class Tez(System):

    module = tez
    once_per_suite = True

    def __init__(self, config):
        self.config = config

    def install(self):
        self.set_config()
        if 'install' not in self.skip_targets:
            execute(tez.install)

    def configure(self):
        self.set_config()
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
        self.set_config()
        if 'install' not in self.skip_targets:
            execute(flink.install)

    def configure(self):
        self.set_config()
        if 'configure' not in self.skip_targets:
            execute(flink.configure)
        execute(flink.pull)

    def reset(self):
        pass

    def start(self):
        self.set_config()
        execute(flink.master)
        execute(flink.slaves)

    def stop(self):
        self.set_config()
        execute(flink.slaves, 'stop')
        execute(flink.master, 'stop')

    def save_log(self, unique_full_path):
        self.set_config()
        execute(flink.copy_log_master, unique_full_path)
        execute(flink.copy_log_slaves, unique_full_path)

    def __str__(self):
        return "flink"


class FlinkYarn(Flink):

    def start(self):
        self.set_config()
        execute(flink.master, yarn=True)

    def stop(self):
        execute(flink.master, 'stop', yarn=True)

    def save_log(self, unique_full_path):
        execute(flink.copy_log_master, unique_full_path, yarn=True)
        execute(flink.copy_log_slaves, unique_full_path, yarn=True)

    def __str__(self):
        return "flink-yarn"