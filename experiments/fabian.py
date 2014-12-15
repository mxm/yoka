from core.lib import Experiment

from time import time

from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

class HadoopCompatibility(Experiment):
    """params: in out list_of_words"""

    def __init__(self):
        pass

    def setup(self):
        pass

    def run(self):
        out_path = "%s/tmp/out_%d" % (get_hdfs_address(), int(time()))
        in_path = "%s/text" % get_hdfs_address()
        def code():
            run_jar("experiments/fabian_files/",
                    "flink-hadoop-compatibility-0.8-incubating-SNAPSHOT.jar",
                    args = [in_path, out_path],
                    clazz = "org.apache.flink.hadoopcompatibility.mapred.example.HadoopMapredCompatWordCount",
                    upload=True)
        master(code)

    def shutdown(self):
        pass

