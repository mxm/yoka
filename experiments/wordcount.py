from core.lib import Experiment

from cluster.maintenance import install
from cluster.utils import master, render_template, exec_bash
from cluster.hadoop import copy_to_hdfs, delete_from_hdfs

from cluster.flink import run_jar, get_flink_dist_path
from cluster.hadoop import get_hdfs_address

from core.utils import GitRepository

from time import time

from fabric.api import env

class WordCountFromJar(Experiment):

    def __init__(self, params):
        self.params = params

    def setup(self):
        # generate wc data
        master(lambda: install("wget"))
        master(lambda: install("ruby"))
        master(lambda: install("bzip2"))
        master(lambda: install("aspell"))
        generate_wc_data = render_template(
            "experiments/wordcount_files/gen_wc_data.sh.mustache",
            self.params
        )
        master(lambda: exec_bash(generate_wc_data))
        master(lambda: copy_to_hdfs("/tmp/wc-data/generated-wc.txt",
                                    "generated-wc.txt"))


    def run(self):
        def code():
            run_jar(path = "experiments/wordcount_files/",
                              jar_name = "flink-java-examples-0.8-incubating-SNAPSHOT-WordCount.jar",
                              args = [
                                "hdfs://%s:50040/generated-wc.txt" % env.master,
                                "hdfs://%s:50040/tmp/wc-out/" % env.master
                              ],
                              upload=True
            )
        master(code)

    def shutdown(self):
        master("rm -rf /tmp/wc-data/generated-wc.txt")
        master(lambda: delete_from_hdfs("generated-wc.txt"))
        master(lambda: delete_from_hdfs("/tmp/wc-out"))


class WordCount(Experiment):
    """params: <text path> <result path>"""

    def __init__(self):
        pass

    def setup(self):
        pass

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        wordcount_out = "%s/tmp/wc_out_%d" % (get_hdfs_address(), int(time()))
        def code():
            run_jar("%s/examples/" % get_flink_dist_path(),
                    "flink-java-*WordCount.jar",
                    args = [wordcount_in, wordcount_out],
                    clazz = "org.apache.flink.examples.java.wordcount.WordCount")
        master(code)

    def shutdown(self):
        pass



class DataFlowExperiment(Experiment):
    repo = GitRepository("https://github.com/aljoscha/flink-dataflow.git", "aljoschas_repo")


class DataFlowWordCount(DataFlowExperiment):
    """params: <text path> <result path>"""

    def __init__(self):
        pass

    def setup(self):
        self.repo.clone()
        self.repo.checkout("perf")
        self.repo.maven("clean package -DskipTests")

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        wordcount_out = "%s/tmp/wc_out_%d" % (get_hdfs_address(), int(time()))
        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = [wordcount_in, wordcount_out],
                    clazz = "com.dataartisans.flink.dataflow.examples.DataflowWordCount")
        master(code)

    def shutdown(self):
        pass


class WordCountSlow(DataFlowExperiment):
    """Slow WordCount for comparison with the Google Dataflow API"""

    def __init__(self):
        pass

    def setup(self):
        self.repo.clone()
        self.repo.checkout("perf")
        self.repo.maven("clean package -DskipTests")

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        wordcount_out = "%s/tmp/wc_out_%d" % (get_hdfs_address(), int(time()))
        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = [wordcount_in, wordcount_out],
                    clazz = "com.dataartisans.flink.dataflow.examples.FlinkWordCount")
        master(code)

    def shutdown(self):
        pass
