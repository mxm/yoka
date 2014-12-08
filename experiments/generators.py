from fabric.api import env
from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

from core.systems import Flink
from configs import flink_config

from core.lib import Experiment, Benchmark
from core.utils import GitRepository

flink_perf_repo = "https://github.com/project-flink/flink-perf"

class Text(Experiment):
    """
     int dop = Integer.valueOf(args[0]);
     String outPath = args[1];
     long finalSizeGB = Integer.valueOf(args[2]);
    """

    dop = 4
    size_gb = 1

    def setup(self):
        self.out_path = get_hdfs_address() + "/text"
        pass

    def run(self):
        repo = GitRepository(flink_perf_repo, "flink-perf")
        repo.clone()
        repo.checkout("master")
        repo.maven("clean install")


        fun = lambda: run_jar("%s/flink-jobs/target" % repo.get_absolute_path(),
                              "flink-jobs-*.jar",
                              args = [self.dop, self.out_path, self.size_gb],
                              clazz = "com.github.projectflink.generators.Text")

        master(fun)

    def shutdown(self):
        pass

systems = [Flink(flink_config)]

text_generator = Benchmark('text_generator',systems, Text())
