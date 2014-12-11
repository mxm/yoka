from core.lib import Benchmark, Generator, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount_new import WordCountNew
from experiments.grep import Grep

# import data generators for benchmarks
from experiments import generators

# Aljoscha's project
compute_engine_config['project_name'] = "astral-sorter-757"
# machine prefix
compute_engine_config['prefix'] = "max-benchmark-"
# 2 cores 7.5GB RAM
compute_engine_config['machine_type'] = "n1-standard-2"
# 16 workers + 1 master
compute_engine_config['num_workers'] = 16
compute_engine_config['disk_space_gb'] = 200

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

benchmarks = [
    Benchmark(
        id = "Grep",
        systems = [flink],
        experiment = Grep(),
        times = 3
    ),

    Benchmark(
        id = "WordCount-new",
        systems = [flink],
        experiment = WordCountNew(),
        times = 3
    ),
]


generators = [
    Generator(
        id = "TextGenerator",
        systems = [flink],
        experiment = generators.Text(
            size_gb = 1024, # 1 tB of data
            dop = compute_engine_config['num_workers']
        )
    )
]

suite = ClusterSuite("DefaultSuite", cluster, systems, generators, benchmarks)

# retry 1 time if cluster setup fails
# do not shutdown the cluster on failures
suite.execute(retry_setup=1, shutdown_on_failure=False)
