from core.lib import Benchmark, Generator, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.fabian import HadoopCompatibility

# import data generators for benchmarks
from experiments import generators

# Aljoscha's project
compute_engine_config['project_name'] = "astral-sorter-757"
# machine prefix
compute_engine_config['prefix'] = "fabian-benchmark-"
# 2 cores 7.5GB RAM
compute_engine_config['machine_type'] = "n1-standard-2"
# num cores to use
compute_engine_config['num_cores'] = 2
# 16 workers + 1 master
compute_engine_config['num_workers'] = 8
compute_engine_config['disk_space_gb'] = 100

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

benchmarks = [
    Benchmark(
        id = "HadoopCompatibility",
        systems = [flink],
        experiment = HadoopCompatibility(),
        times = 1
    ),
]


generators = [
    Generator(
        id = "TextGenerator",
        systems = [flink],
        experiment = generators.Text(
            size_gb = 10,
            dop = compute_engine_config['num_workers'] * compute_engine_config['num_cores']
        )
    )
]

suite = ClusterSuite("DefaultSuite", cluster, systems, generators, benchmarks)

suite.execute()
