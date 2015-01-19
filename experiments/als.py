from core.lib import Experiment

from time import time

from cluster.utils import master
from cluster.flink import run_jar
from cluster.hadoop import get_hdfs_address

import generators

class ALS(Experiment):
    """

    flink run -v -c \
    com.github.projectflink.als.ALSJoinBlocking -p 320 \
    ~/flink-jobs-0.1-SNAPSHOT.jar master 15 1 10 $BLOCKS rand \
    hdfs:///als-temp/ \
    hdfs:///als-benchmark4000000-500000-300/ \
    hdfs:///als-results4000000-500000-300/

    Die Parameter haben folgende Bedeutung:

    Latent factors
    Regularization constant
    Number iterations
    Number blocks
    Random seed
    temp directory wo er Zwischenergebnisse reinschreibt
    Input
    Output

    flink run -v -c \
    com.github.projectflink.als.ALSJoinBlocking -p 320 \
    ~/flink-jobs-0.1-SNAPSHOT.jar master 15 1 10 100 rand \
    hdfs:///als-temp/ \
    hdfs:///als-benchmark400000-50000-100/ \
    hdfs:///als-results400000-500000-100/

    """
    
    def __init__(self):
        pass

    def setup(self):
        pass

    def run(self):
        # TODO get from generator directly
        als_in = "%s/als-benchmark800000-100000-400" % get_hdfs_address()
        als_out = "%s/tmp/als_out_%d" % (get_hdfs_address(), int(time()))

        def code():
            run_jar("%s/flink-jobs/target" % generators.ALS.repo.get_absolute_path(),
                    "flink-jobs-*.jar",
                    args = [
                        "master",
                        15, 1, 10, 100, "rand",
                        "%s/als-temp/" % get_hdfs_address(),
                        als_in,
                        als_out
                    ],
                    clazz = "com.github.projectflink.als.ALSJoinBlocking")
            
        master(code)

    def shutdown(self):
        pass
