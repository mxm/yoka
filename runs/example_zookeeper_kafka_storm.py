from core.lib import ClusterSuite

# import cluster and systems classes
from core.clusters import Local
from core.systems import Hadoop, Flink, Zookeeper, Storm, Kafka

# import standard configs
from configs import local_cluster_config, hadoop_config, zookeeper_config, storm_config, kafka_config


cluster = Local(local_cluster_config)
hadoop = Hadoop(hadoop_config)

zookeeper = Zookeeper(zookeeper_config)
storm = Storm(storm_config)
kafka = Kafka(kafka_config)

#systems = [hadoop, zookeeper, storm, kafka]
systems = [zookeeper, storm, kafka]

benchmarks = [
]

generators = [
]

suite = ClusterSuite("ZookeeperSuite", cluster, systems, generators, benchmarks)

suite.setup()
