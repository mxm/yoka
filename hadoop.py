from fabric.decorators import task, roles, runs_once, parallel
from fabric.api import env, cd, run, put, sudo
import config as conf
from maintenance import pull_from_master, set_java_home
from utils import process_template


@task
@roles('master')
def install_hadoop():
    run("curl %s | tar xz" % conf.HADOOP_SOURCE)
    run("mv hadoop* %s" % conf.HADOOP_PATH)

@task
@parallel
def create_hdfs_dir():
    run("mkdir %s" % conf.HDFS_DATAPATH)


@task
@roles('master')
def configure_hdfs():
    set_java_home("%s/%s/hadoop-env.sh" % (conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH))
    context =  {'master': env.master,
                'namenode_path': conf.HDFS_NAMENODE_PATH,
                'datanode_path': conf.HDFS_DATANODE_PATH
    }
    destination = conf.HADOOP_PATH + "/" + conf.HDFS_CONFIG_PATH
    process_template("hdfs", "hdfs-site.xml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("hdfs", "slaves.mustache", context2, destination)

@task
@roles('master')
def format_hdfs_master():
    run("%s/bin/hdfs namenode -format" % conf.HADOOP_PATH)

@task
@roles('slaves')
#@parallel
def pull_hadoop():
    pull_from_master(conf.HADOOP_PATH)
    #pull_from_master(conf.HDFS_DATAPATH)

@task
@roles('master')
def hdfs_master(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s namenode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH, action))

@task
@roles('slaves')
def hdfs_slaves(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s datanode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH, action))
