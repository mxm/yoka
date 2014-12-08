from core.lib import Experiment

from time import time

from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

class Grep(Experiment):
    """params: in out list_of_words"""

    def __init__(self):
        pass

    def setup(self):
        pass

    def run(self):
        grep_out = "%s/tmp/grep_out_%d" % (get_hdfs_address(), int(time()))
        grep_in = "%s/text" % get_hdfs_address()
        def code():
            run_jar("~/flink-perf/flink-jobs/target",
                    "flink-jobs-*.jar",
                    args = [grep_in, grep_out, "these", "are", "test", "words"],
                    clazz = "com.github.projectflink.grep.GrepJob")
        master(code)

    def shutdown(self):
        pass
