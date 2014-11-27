from fabric.decorators import task, roles, runs_once, parallel
from fabric.api import env, cd, run, put, sudo
import config as conf
from maintenance import pull_from_master, set_java_home
from utils import process_template


@task
@roles('master')
def install():
    run("curl %s | tar xz" % conf.HADOOP_SOURCE)
    run("mv hadoop* %s" % conf.HADOOP_PATH)

def format_hdfs_master():
    run("mkdir %s" % conf.HDFS_DATAPATH)
    run("%s/bin/hdfs namenode -format" % conf.HADOOP_PATH)

@task
@roles('master')
def configure():
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
    format_hdfs_master()

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(conf.HADOOP_PATH)

@task
@roles('master')
def master(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s namenode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH, action))

@task
@roles('slaves')
def slaves(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s datanode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH, action))
