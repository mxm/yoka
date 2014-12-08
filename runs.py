import sys

from lib import Benchmark, ClusterSuite

# import cluster and systems classes
from clusters import ComputeEngine
from systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount import WordCount


cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

custom_flink_conf = dict(flink_config)
custom_flink_conf['git_commit'] = "858d1bccf957bf36c04ab011ec9a26933109086c"
custom_flink_conf['taskmanager_num_buffers'] = 1024

custom_flink = Flink(custom_flink_conf)

benchmarks = [
    Benchmark(
        id = "WordCount1000",
        systems = [hadoop, flink],
        experiment = WordCount({
            'num_lines' : 1000
        }),
        times = 5
    ),

    Benchmark(
        id = "WordCount1000-custom",
        systems = [hadoop, custom_flink],
        experiment = WordCount({
            'num_lines' : 1000
        }),
        times = 3
    )
]
# data_generator
# data_generator.generate()
suite = ClusterSuite("DefaultSuite", cluster, systems, benchmarks)

suite.setup()
suite.run()
""""while True:
    try:
        suite.run()
        break
    except:
        print sys.exc_info()[0]
"""
suite.shutdown()
