from time import time
from core.lib import ExperimentWithGenerator

from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

import generators

class Avro(ExperimentWithGenerator):
    """
   - Run Prepare ./flink run -v -p 152 -c com.github.projectflink.avro.Prepare ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar hdfs:///user/robert/datasets/tpch1/lineitems.csv hdfs:///user/robert/datasets/tpch1-avro/
   - Run Compare. ./flink run -v -p 152 -c com.github.projectflink.avro.CompareJob ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar hdfs:///user/robert/datasets/tpch1-avro/ hdfs:///user/robert/datasets/tpch1/lineitems.csv

    """

    def __init__(self, generator):
        super(Avro, self).__init__(generator)

    def setup(self):
        self.in_path = self.generator.experiment.out
        self.out_path = get_hdfs_address() + "/avro-benchmark/tpch1-avro%d" % int(time())

        def code():
            run_jar("%s/flink-jobs/target" % generators.Avro.repo.get_absolute_path(),
                "flink-jobs-*.jar",
                args = [
                    self.in_path,
                    self.out_path
                ],
                clazz = "com.github.projectflink.avro.Prepare")

        master(code)

    def run(self):

        def code():           
            run_jar("%s/flink-jobs/target" % generators.Avro.repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args = [
                        self.out_path,
                        self.in_path
                    ],
                    clazz = "com.github.projectflink.avro.CompareJob")

        master(code)

    def shutdown(self):
        # TODO clean up
        pass
