from core.lib import Experiment

from cluster.maintenance import install
from cluster.utils import master, render_template, exec_bash
from cluster.hadoop import copy_to_hdfs, delete_from_hdfs

from cluster.flink import run_jar, get_flink_dist_path, get_flink_path
from cluster.hadoop import get_hdfs_address

from core.utils import GitRepository

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
        self.wordcount_in = "%s/text" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

    def run(self):

        def code():
            run_jar("%s/examples/" % get_flink_dist_path(),
                    "flink-java*WordCount.jar",
                    args = [self.wordcount_in, self.wordcount_out],
                    clazz = "org.apache.flink.examples.java.wordcount.WordCount")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))


class StreamingWordCount(Experiment):
    """params: <text path> <result path>"""

    def __init__(self):
        pass

    def setup(self):
        self.wordcount_in = "%s/text2" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

    def run(self):

        def code():
            run_jar("%s/flink-staging/flink-streaming/flink-streaming-examples/target/" % get_flink_path(),
                    "flink-streaming*WordCount.jar",
                    args = [self.wordcount_in, self.wordcount_out],
                    clazz = "org.apache.flink.streaming.examples.wordcount.WordCount")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))

class WindowWordCount(Experiment):
    """params: <text path> <result path>"""

    def __init__(self):
        pass

    def setup(self):
        self.wordcount_in = "%s/text" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

    def run(self):

        def code():
            run_jar("%s/flink-staging/flink-streaming/flink-streaming-examples/target/" % get_flink_path(),
                    "flink-streaming*WindowWordCount.jar",
                    args = [self.wordcount_in, self.wordcount_out, 10000],
                    clazz = "org.apache.flink.streaming.examples.windowing.WindowWordCount")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))


class DataFlowExperiment(Experiment):
    repo = GitRepository("https://github.com/mxm/flink-dataflow.git", "dataflow-repo")


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
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = ["--", # Flink 0.8 way of specifying options to user programs
                            "--input=%s" % wordcount_in,
                            "--output=%s" % self.wordcount_out],
                    clazz = "com.dataartisans.flink.dataflow.examples.DataflowWordCount")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))


class FlinkWordCount(DataFlowExperiment):
    """
        Either implicit or explicit Flink WordCount for comparison with Google Dataflow API.
        In the implicit version, the combiner is specified in the GroupReduceFunction. In the explicit
        case, a custom combiner via GroupCombine is specified.
    """
    implicit_clazz = "com.dataartisans.flink.dataflow.examples.FlinkWordCountImplicitCombine"
    explicit_clazz = "com.dataartisans.flink.dataflow.examples.FlinkWordCountExplicitCombine"

    def __init__(self, implicit_combine=True):
        self.implicit_combine = implicit_combine

    def setup(self):
        self.repo.clone()
        self.repo.checkout("perf")
        self.repo.maven("clean package -DskipTests")

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = [wordcount_in, self.wordcount_out],
                    clazz = self.implicit_clazz if self.implicit_combine else self.explicit_clazz)
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))

class StreamingDataFlowExperiment(Experiment):
    repo = GitRepository("https://github.com/mbalassi/flink-dataflow.git", "dataflow-repo")

class WindowWordCountFlinkDataFlow(StreamingDataFlowExperiment):

    def __init__(self):
        pass

    def setup(self):
        self.repo.clone()
        self.repo.checkout("perf")
        self.repo.maven("clean package -DskipTests")

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = [wordcount_in, self.wordcount_out],
                    clazz = "com.dataartisans.flink.dataflow.examples.StreamingPipeline")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))

class WindowWordCountGoogleDataFlow(StreamingDataFlowExperiment):

    def __init__(self):
        pass

    def setup(self):
        self.repo.clone()
        self.repo.checkout("perf")
        self.repo.maven("clean package -DskipTests")

    def run(self):
        wordcount_in = "%s/text" % get_hdfs_address()
        self.wordcount_out = "%s/tmp/wc_out" % get_hdfs_address()

        def code():
            run_jar("%s/target/" % self.repo.get_absolute_path(),
                    "flink-dataflow-*-SNAPSHOT.jar",
                    args = [wordcount_in, self.wordcount_out],
                    clazz = "com.dataartisans.flink.dataflow.GoogleStreamingPipeline.examples")
        master(code)

    def shutdown(self):
        master(lambda: delete_from_hdfs(self.wordcount_out))
