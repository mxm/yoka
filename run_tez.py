from core.lib import Benchmark, ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Tez

# import standard configs
from configs import compute_engine_config, hadoop_config, tez_config

# import experiment's main class


# import data generators for benchmarks
from experiments import generators

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
tez = Tez(tez_config)

systems = [hadoop, tez]

benchmarks = [

]

generators = []

suite = ClusterSuite("DefaultSuite", cluster, systems, generators, benchmarks)

suite.execute()
