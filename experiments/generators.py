from cluster.utils import master, slaves, master_slaves
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address
from cluster.maintenance import install

from core.lib import Generator
from core.utils import GitRepository

flink_perf_repo = "https://github.com/project-flink/flink-perf"

class FlinkPerf(Generator):
    repo = GitRepository(flink_perf_repo, "flink-perf")


class Text(FlinkPerf):
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

        self.repo.clone()
        self.repo.checkout("master")
        self.repo.maven("clean package")

    def run(self):

        def code():
            run_jar("%s/flink-jobs/target" % self.repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args=[self.dop, self.out_path, self.size_gb],
                    clazz="com.github.projectflink.generators.Text")

        master(code)

    def shutdown(self):
        pass
    

class ALS(FlinkPerf):

    """
    
    Data generation:
    
    flink  run -v -c \
    com.github.projectflink.als.ALSDataGeneration -p 320 \
    ~/flink-jobs-0.1-SNAPSHOT.jar 4000000 500000 20 4 300 100 hdfs:///als-benchmark
    
    Das erstellt die Testdaten in hdfs://als-benchmark4000000-500000-300
    
    Die Parameter haben folgende Bedeutung
    
    Number of rows
    Number of columns
    mean of entry
    variance of entry
    mean of number of row entries
    variance of number of row entries
    output directory which gets the parameters appended
    
        flink  run -v -c \
        com.github.projectflink.als.ALSDataGeneration -p 320 \
        ~/flink-jobs-0.1-SNAPSHOT.jar 400000 50000 20 4 200 50 hdfs:///als-benchmark
    
    """
    
    def __init__(self, num_rows, num_cols,
                 mean_entry, variance_entry,
                 mean_num_row_entries, variance_num_row_entries):
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.mean_entry = mean_entry
        self.variance_entry = variance_entry
        self.mean_num_row_entries = mean_num_row_entries
        self.variance_num_row_entries = variance_num_row_entries

    def setup(self):
        master_slaves(lambda: install("libgfortran3"))
        self.out_path = get_hdfs_address() + "/als-benchmark"

        self.repo.clone()
        self.repo.checkout("master")
        self.repo.maven("clean package")

    def run(self):

        def code():
            run_jar("%s/flink-jobs/target" % self.repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args=[
                        self.num_rows, self.num_cols,
                        self.mean_entry, self.variance_entry,
                        self.mean_num_row_entries, self.variance_num_row_entries,
                        self.out_path
                    ],
                    clazz="com.github.projectflink.als.ALSDataGeneration")

        master(code)

    def shutdown(self):
        pass


class Avro(FlinkPerf):
    """
    - Run Generate Lineitems: ./flink run -v -p 152 -c com.github.projectflink.avro.GenerateLineitems ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar -p 152 -o hdfs:///user/robert/datasets/tpch1/
    """

    def __init__(self, parallelism, scale_factor=1.0):
        self.scale_factor = scale_factor
        self.parallelism = parallelism

    def setup(self):

        self.repo.clone()
        self.repo.checkout("master")
        self.repo.maven("clean package")

    def run(self):
        self.out = get_hdfs_address() + "/avro-benchmark/tpch1/" # + lineitems.csv

        def code():
            run_jar("%s/flink-jobs/target" % self.repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args=[
                        "-s", self.scale_factor,
                        "-p", self.parallelism,
                        "-o", self.out,
                        ],
                    clazz="com.github.projectflink.avro.GenerateLineitems"
            )

        master(code)
        # update path for benchmark
        self.out += "lineitems.csv"

    def shutdown(self):
        pass
