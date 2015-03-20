from core.lib import ClusterSuite

# import cluster and systems classes
from core.clusters import Local
from core.systems import Hadoop, Flink, Zookeeper, Storm

# import standard configs
from configs import local_cluster_config, hadoop_config, zookeeper_config, storm_config


cluster = Local(local_cluster_config)
hadoop = Hadoop(hadoop_config)

zookeeper = Zookeeper(zookeeper_config)
storm = Storm(storm_config)

systems = [hadoop, zookeeper, storm]

benchmarks = [
]


generators = [
]

suite = ClusterSuite("ZookeeperSuite", cluster, systems, generators, benchmarks)

suite.setup()
