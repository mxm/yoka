from core.lib import ClusterSuite

# import cluster and systems classes
from core.clusters import ComputeEngine
from core.systems import Hadoop, Flink, Zookeeper, Storm, Kafka

# import standard configs
from configs import compute_engine_config, hadoop_config, zookeeper_config, storm_config, kafka_config

# Aljoscha's project
compute_engine_config['project_name'] = "astral-sorter-757"
# machine prefix
compute_engine_config['prefix'] = "kafka-benchmark-"
# 2 cores 7.5GB RAM
compute_engine_config['machine_type'] = "n1-standard-2"
compute_engine_config['num_cores'] = 2
compute_engine_config['size_mem'] = 7500
compute_engine_config['num_workers'] = 10
compute_engine_config['disk_space_gb'] = 200

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)

zookeeper = Zookeeper(zookeeper_config)
storm = Storm(storm_config)
kafka = Kafka(kafka_config)

systems = [hadoop, zookeeper, storm, kafka]

benchmarks = [
]

generators = [
]

suite = ClusterSuite("ZookeeperSuite", cluster, systems, generators, benchmarks)

suite.setup()
