from __future__ import with_statement
from fabric.decorators import task, roles, runs_once, parallel
from fabric.api import env, cd, run, put
import config as conf
from maintenance import pull_from_master, set_java_home

import pystache

@task
@roles('master')
def install_hadoop():
    run("curl %s | tar xz" % conf.HADOOP_SOURCE)
    run("mv hadoop* %s" % conf.HADOOP_PATH)

@task
@parallel
def create_hdfs_dir():
    run("mkdir %s" % conf.HDFS_DATAPATH)

def process_template(template, context):
    renderer = pystache.Renderer()
    template_path = "%s/hdfs/%s" % (conf.TEMPLATE_PATH, template)
    config_content = renderer.render_path(
        template_path,
        context
    )
    src = "%s/%s" % (conf.TMP, template[:-9])
    f = open(src, 'w')
    f.write(config_content)
    f.close()
    put(src, "%s/%s/" % (conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH))

@task
@roles('master')
def configure_hdfs():
    set_java_home("%s/%s/hadoop-env.sh" % (conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH))
    context =  {'master': 'localhost', 'datapath': conf.HDFS_DATAPATH}
    process_template("hdfs-site.xml.mustache", context)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("slaves.mustache", context2)

@task
@roles('master')
def format_hdfs():
    run("%s/bin/hadoop namenode -format" % conf.HADOOP_PATH)

@task
@roles('slaves')
@parallel
def pull_hadoop():
    pull_from_master(conf.HADOOP_PATH)
    pull_from_master(conf.HDFS_DATAPATH)

@task
@roles('master')
def start_hdfs_master():
    sudo("%s/sbin/hdfs --config %s/%s  --script start namenode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH))

@task
@roles('slaves')
def start_hdfs_slaves():
    sudo("%s/sbin/hdfs --config %s/%s  --script start datanode" % (conf.HADOOP_PATH, conf.HADOOP_PATH, conf.HDFS_CONFIG_PATH))
