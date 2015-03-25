from core.lib import Benchmark, Generator, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink

# import standard configs
from configs import compute_engine_config, hadoop_config, flink_config

# import experiment's main class
from experiments.wordcount import DataFlowWordCount, FlinkWordCount

# import data generators for benchmarks
from experiments import generators

# Aljoscha's project
compute_engine_config['project_name'] = "astral-sorter-757"
# machine prefix
compute_engine_config['prefix'] = "dataflow-benchmark-"
# 2 cores 7.5GB RAM
compute_engine_config['machine_type'] = "n1-standard-2"
# num cores to use
compute_engine_config['num_cores'] = 2
compute_engine_config['size_mem'] = 7500
# 16 workers + 1 master
compute_engine_config['num_workers'] = 10
compute_engine_config['disk_space_gb'] = 200

dop = compute_engine_config['num_workers'] * compute_engine_config['num_cores']

flink_config['git_repository'] = "https://github.com/apache/flink.git"
flink_config['git_commit'] = "master"
flink_config['num_task_slots'] = compute_engine_config['num_cores']
flink_config['taskmanager_heap'] = 5120 #5gb
flink_config['jobmanager_heap'] = 5120
flink_config['parallelization'] = dop


cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

generators = [

    Generator(
        id = "TextGenerator",
        systems = [flink],
        experiment = generators.Text(
            size_gb = 150,
            dop = dop,
            git_branch = "master"
        )
    ),

]

benchmarks = [

    Benchmark(
        id = "WordCountDataFlow",
        systems = [flink],
        experiment = DataFlowWordCount(),
        times = 3
    ),

    Benchmark(
        id = "ImplicitFlinkWordCount",
        systems = [flink],
        experiment = FlinkWordCount(implicit_combine=True),
        times = 3
    ),

    Benchmark(
        id = "ExplicitFlinkWordCount",
        systems = [flink],
        experiment = FlinkWordCount(implicit_combine=False),
        times = 3
    ),

]

suite = ClusterSuite("DataFlowSuite", cluster, systems, generators, benchmarks)

suite.execute(retry_setup=0,
              shutdown_on_failure=True,
              email_results=True)
