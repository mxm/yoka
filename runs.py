import sys

from core.lib import Benchmark, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount import WordCount
from experiments.wordcount_new import WordCountNew
from experiments.grep import Grep

# import data generators for benchmarks
from experiments import generators

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

custom_flink_config = flink_config.copy()
custom_flink_config['git_commit'] = "858d1bccf957bf36c04ab011ec9a26933109086c"
custom_flink_config['taskmanager_num_buffers'] = 1024

custom_flink = Flink(custom_flink_config)

benchmarks = [
    # Normal benchmark
    # Benchmark(
    #     id = "WordCount1000",
    #     systems = [flink],
    #     experiment = WordCount({
    #         'num_lines' : 1000
    #     }),
    #     times = 5
    # ),
    # Custom Flink version benchmark
    # Benchmark(
    #     id = "WordCount1000-custom",
    #     systems = [custom_flink],
    #     experiment = WordCount({
    #         'num_lines' : 1000
    #     }),
    #     times = 3
    # ),
    Benchmark(
        id = "Grep",
        systems = [flink],
        experiment = Grep(),
        times = 2
    ),

    Benchmark(
        id = "WordCount-new",
        systems = [flink],
        experiment = WordCountNew(),
        times = 2
    ),
]


generators = [generators.text_generator]

suite = ClusterSuite("DefaultSuite", cluster, systems, generators, benchmarks)

suite.execute()
