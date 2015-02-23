from core.lib import Benchmark, Generator, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount import WordCount

# import data generators for benchmarks
from experiments import generators

# Aljoscha's project
compute_engine_config['project_name'] = "astral-sorter-757"
# machine prefix
compute_engine_config['prefix'] = "offheap-benchmark-"
# 2 cores 7.5GB RAM
compute_engine_config['machine_type'] = "n1-standard-2"
# num cores to use
compute_engine_config['num_cores'] = 2
# 16 workers + 1 master
compute_engine_config['num_workers'] = 10
compute_engine_config['disk_space_gb'] = 100

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)

flink_config['git_repository'] = "https://github.com/mxm/flink.git"
flink_config['git_commit'] = "aba76171fef41e2c987913c32fefafc55ef635f6"
flink = Flink(flink_config)

flink_config_custom = flink_config.copy()
flink_config_custom['git_commit'] = "off_heap_rebased"
flink_config_custom['extra_config_entries'] = [
    { 'entry' : "taskmanager.memory.directAllocation: true" }
]
flink_custom = Flink(flink_config_custom)

systems = [hadoop, flink]

benchmarks = [
    Benchmark(
        id = "WordCount-heap",
        systems = [flink],
        experiment = WordCount(),
        times = 1
    ),
    Benchmark(
        id = "WordCount-offheap",
        systems = [flink_custom],
        experiment = WordCount(),
        times = 1
    ),
]


generators = [
    Generator(
        id = "TextGenerator",
        systems = [flink],
        experiment = generators.Text(
            size_gb = 150, # 512 gB of data
            dop = compute_engine_config['num_workers'] * compute_engine_config['num_cores']
        )
    )
]

suite = ClusterSuite("DefaultSuite", cluster, systems, generators, benchmarks)

suite.execute(retry_setup=0,
              shutdown_on_failure=True,
              email_results=True)
