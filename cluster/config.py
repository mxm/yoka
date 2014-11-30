import getpass

USER = getpass.getuser()

NUM_WORKERS = 2

GCE_PROJECT_NAME = "braided-keel-768"
GCE_ZONE = "europe-west1-b"
GCE_MACHINE_TYPE = "n1-standard-2"
GCE_DISK_IMAGE = "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/backports-debian-7-wheezy-v20141108"
GCE_MACHINE_PREFIX = "benchmark-"

FLINK_PATH = "/home/%s/flink" % USER
FLINK_REPOSITORY = "https://github.com/apache/incubator-flink"
FLINK_COMMIT = "master"

FLINK_NUMBER_TASK_SLOTS = 8
FLINK_PARALLELIZATION = 1
FLINK_JOBMANAGER_HEAP = 256
FLINK_TASKMANAGER_HEAP = 512
FLINK_TASKMANAGER_NUM_BUFFERS = 2048

PEEL_DIR = "/home/max/Dev/DataArtisans/peel"

HADOOP_SOURCE = "http://mirror.arcor-online.net/www.apache.org/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz"
HADOOP_PATH="/home/%s/hadoop" % USER

JAVA_HOME = "/usr/lib/jvm/java-1.7.0-openjdk-amd64"

CLUSTER_TEMPLATE_PATH="cluster/templates"

HDFS_CONFIG_PATH = "etc/hadoop"
HDFS_NAMENODE_PATH = "/home/%s/hdfs-namenode" % USER
HDFS_DATANODE_PATH = "/home/%s/hdfs-datanode" % USER

TMP="/tmp"
