from core.lib import Benchmark, Generator, ClusterSuite

# import cluster and systems classes
from core.clusters import Local
from core.systems import Hadoop, Flink

# import standard configs
from configs import local_cluster_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount import WordCount

# import data generators for benchmarks
from experiments import generators

cluster = Local(local_cluster_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

benchmarks = [
    # Normal benchmark
    Benchmark(
        id = "WordCount1000",
        systems = [flink],
        experiment = WordCount({
            'num_lines' : 1000
        }),
        times = 5
    ),
]


generators = [
]

suite = ClusterSuite("LocalSuite", cluster, systems, generators, benchmarks)

suite.execute()
