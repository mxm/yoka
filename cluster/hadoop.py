from fabric.decorators import task, roles, parallel
from fabric.api import env, run, sudo
from maintenance import pull_from_master, set_java_home
from utils import process_template

from configs import hadoop_config as conf

@task
@roles('master')
def install():
    sudo("rm -rf '%s'" % conf['path'])
    run("curl %s | tar xz" % conf['source'])
    run("mv hadoop* %s" % conf['path'])

@roles('master')
def format_hdfs_master():
    run("rm -rf '%s'" % conf['namenode_path'])
    run("mkdir %s" % conf['namenode_path'])
    run("%s/bin/hdfs namenode -format" % conf['path'])

@task
@roles('slaves')
def delete_data_slaves():
    sudo("rm -rf '%s'" % conf['datanode_path'])

@task
@roles('master')
def configure():
    set_java_home("%s/%s/hadoop-env.sh" % (conf['path'], conf['config_path']))
    context =  {'master': env.master,
                'namenode_path': conf['namenode_path'],
                'datanode_path': conf['datanode_path']
    }
    destination = conf['path'] + "/" + conf['config_path']
    process_template("hadoop", "hdfs-site.xml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("hadoop", "slaves.mustache", context2, destination)
    format_hdfs_master()

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(conf['path'])

@task
@roles('master')
def master(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s namenode" % (conf['path'], conf['path'], conf['config_path'], action))

@task
@roles('slaves')
@parallel
def slaves(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s datanode" % (conf['path'], conf['path'], conf['config_path'], action))


def copy_to_hdfs(src, dest):
    run("%s/bin/hdfs dfs -put -f '%s' 'hdfs://%s:50040/%s'"
        % (conf['path'], src, env.master, dest)
    )

def delete_from_hdfs(path):
     run("%s/bin/hdfs dfs -rm 'hdfs://%s:50040/%s'"
        % (conf['path'], env.master, path)
     )

def get_hdfs_address():
    return "hdfs://%s:50040/" % env.master
