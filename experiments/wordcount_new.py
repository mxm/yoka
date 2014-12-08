from core.lib import Experiment

from time import time

from cluster.utils import master
from cluster.flink import run_jar, get_flink_dist_path
from cluster.hadoop import get_hdfs_address

class WordCountNew(Experiment):
    """params: <text path> <result path>"""

    def __init__(self):
        pass

    def setup(self):
        pass

    def run(self):
        wordcount_in = "%s/tmp/grep_out_%d" % (get_hdfs_address(), int(time()))
        wordcount_out = "%s/text" % get_hdfs_address()
        def code():
            run_jar("%s/examples/" % get_flink_dist_path(),
                    "flink-java-*WordCount.jar",
                    args = [wordcount_out, wordcount_in],
                    clazz = "org.apache.flink.examples.java.wordcount.WordCount")
        master(code)

    def shutdown(self):
        pass
