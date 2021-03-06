import random
from time import sleep
from core.lib import System

from cluster import hadoop, flink, tez, zookeeper, storm, kafka
from fabric.api import execute, env

from log import get_logger

logger = get_logger("systems")

class Hadoop(System):
    """
    Installs, configures, and starts
    Hadoop v2 including HDFS, YARN, and MapReduce
    """

    module = hadoop
    once_per_suite = True
    priority = 0

    def __init__(self, config):
        super(Hadoop, self).__init__(config)

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
        super(Tez, self).__init__(config)

    def install(self):
        self.set_config()
        execute(tez.install)

    def configure(self):
        self.set_config()
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
        super(Flink, self).__init__(config)

    def install(self):
        self.set_config()
        if 'install' not in self.skip_targets:
            execute(flink.install)

    def configure(self):
        self.set_config()
        if 'configure' not in self.skip_targets:
            execute(flink.configure)
        execute(flink.pull)
        execute(flink.create_temp_dir)

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

    def test_fault_tolerance(self):
        self.set_config()
        random_taskmanager = random.choice(env.roledefs['slaves'])
        execute(flink.kill_taskmanager, host=random_taskmanager)
        sleep(30)
        execute(flink.slaves, "start", host=random_taskmanager)

    def __str__(self):
        return "flink"


class FlinkYarn(Flink):

    def start(self):
        self.set_config()
        execute(flink.master, yarn=True)

    def stop(self):
        execute(flink.master, 'stop', yarn=True)

    def __str__(self):
        return "flink-yarn"


class Zookeeper(System):

    module = zookeeper
    once_per_suite = True
    # should be started before other systems
    priority = -1

    def __init__(self, config):
        super(Zookeeper, self).__init__(config)

    def install(self):
        self.set_config()
        execute(zookeeper.install)

    def configure(self):
        self.set_config()
        execute(zookeeper.pull)
        execute(zookeeper.configure)
        execute(zookeeper.create_myid_file)

    def reset(self):
        pass

    def start(self):
        self.set_config()
        for i in range(self.config['num_instances']):
            if i < len(env.hosts):
                execute(zookeeper.nodes, 'start', host=env.hosts[i])
            else:
                logger.error("Cannot start more zookeeper instances than servers.")

    def stop(self):
        self.set_config()
        execute(zookeeper.nodes, 'stop')

    def save_log(self, unique_full_path):
        pass

    def __str__(self):
        return "zookeeper"


class Kafka(System):

    module = kafka
    once_per_suite = True

    def __init__(self, config):
        super(Kafka, self).__init__(config)

    def install(self):
        self.set_config()
        execute(kafka.install)

    def configure(self):
        self.set_config()
        execute(kafka.pull)
        execute(kafka.configure)

    def reset(self):
        pass

    def start(self):
        self.set_config()
        for i in range(self.config['num_instances']):
            if i < len(env.hosts):
                execute(kafka.nodes, 'start', host=env.hosts[i])
            else:
                logger.error("Cannot start more kafka instances than servers.")

    def stop(self):
        self.set_config()
        execute(kafka.nodes, 'stop')

    def save_log(self, unique_full_path):
        pass

    def __str__(self):
        return "kafka"


class Storm(System):

    module = storm
    # TODO for now this is ok should be changed when integrated with benchmarks
    once_per_suite = True

    def __init__(self, config):
        super(Storm, self).__init__(config)

    def install(self):
        self.set_config()
        execute(storm.install)

    def configure(self):
        self.set_config()
        execute(storm.configure)
        execute(storm.pull)
        execute(storm.create_local_dir)

    def reset(self):
        pass

    def start(self):
        self.set_config()
        execute(storm.master)
        execute(storm.slaves)

    def stop(self):
        self.set_config()
        # TODO
        # storm has not stop method

    def save_log(self, unique_full_path):
        pass

    def __str__(self):
        return "storm"