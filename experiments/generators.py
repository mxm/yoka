from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

from core.systems import Flink
from configs import flink_config

from core.lib import Experiment, Generator
from core.utils import GitRepository

flink_perf_repo = "https://github.com/project-flink/flink-perf"


class Text(Experiment):
    """
     int dop = Integer.valueOf(args[0]);
     String outPath = args[1];
     long finalSizeGB = Integer.valueOf(args[2]);
    """

    def __init__(self, size_gb, dop):
        self.dop = dop
        self.size_gb = size_gb

    def setup(self):
        self.out_path = get_hdfs_address() + "/text"
        pass

    def run(self):
        repo = GitRepository(flink_perf_repo, "flink-perf")
        repo.clone()
        repo.checkout("master")
        repo.maven("clean install")

        def code():
            run_jar("%s/flink-jobs/target" % repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args=[self.dop, self.out_path, self.size_gb],
                    clazz="com.github.projectflink.generators.Text")

        master(code)

    def shutdown(self):
        pass


class ALS(Experiment):

    def setup(self):
        self.out_path = get_hdfs_address() + "/als-benchmark"
        pass

    def run(self):
        repo = GitRepository(flink_perf_repo, "flink-perf")
        repo.clone()
        repo.checkout("master")
        repo.maven("clean install")

        def code():
            run_jar("%s/flink-jobs/target" % repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args=[40000000, 5000000, 8, 2, 700, 300, self.out_path],
                    clazz="com.github.projectflink.als.ALSDataGeneration")

        master(code)

    def shutdown(self):
        pass
