from fabric.decorators import task, roles, parallel
from fabric.api import env, run, sudo, put, cd
from maintenance import pull_from_master, set_java_home
from utils import process_template

from time import sleep

from configs import hadoop_config as conf

namenode_dir = "hdfs-namenode"
datanode_dir = "hdfs-datanode"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % conf['path'])
    run("curl %s | tar xz" % conf['source'])
    run("mv hadoop* %s" % conf['path'])

@roles('master')
def format_hdfs_master():
    run("rm -rf %s/%s" % (conf['data_path'], namenode_dir))
    run("mkdir -p %s/%s" % (conf['data_path'], namenode_dir))
    run("%s/bin/hdfs namenode -format" % conf['path'])

@task
@roles('slaves')
def delete_data_slaves():
    run("rm -rf %s/%s" % (conf['data_path'], datanode_dir))

@task
@roles('master')
def configure():
    set_java_home("%s/etc/hadoop/hadoop-env.sh" % conf['path'])
    # configure hdfs
    context = conf.copy()
    context['master'] = env.master
    context['namenode_path'] = "%s/%s" % (conf['data_path'], namenode_dir)
    context['datanode_path'] = "%s/%s" % (conf['data_path'], datanode_dir)
    destination = conf['path'] + "/etc/hadoop/"
    process_template("hadoop", "hdfs-site.xml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context['slaves'] = slaves
    process_template("hadoop", "slaves.mustache", context, destination)
    format_hdfs_master()
    # configure YARN
    process_template("hadoop", "core-site.xml.mustache", context, destination)
    process_template("hadoop", "yarn-site.xml.mustache", context, destination)
    # configure MapReduce v2
    process_template("hadoop", "mapred-site.xml.mustache", context, destination)

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(conf['path'])

@task
@roles('master')
def master(action="start"):
    run("%s/sbin/hadoop-daemon.sh --config %s/etc/hadoop/ --script hdfs %s namenode" % (conf['path'], conf['path'], action))
    run("%s/sbin/yarn-daemon.sh --config %s/etc/hadoop/ %s resourcemanager" % (conf['path'], conf['path'], action))
    run("%s/sbin/mr-jobhistory-daemon.sh --config %s/etc/hadoop/ %s historyserver" % (conf['path'], conf['path'], action))
    sleep(1)

@task
@roles('slaves')
@parallel
def slaves(action="start"):
    run("%s/sbin/hadoop-daemon.sh --config %s/etc/hadoop/ --script hdfs %s datanode" % (conf['path'], conf['path'], action))
    run("%s/sbin/yarn-daemon.sh --config %s/etc/hadoop/ %s nodemanager" % (conf['path'], conf['path'], action))

def mkdir_hdfs(dir):
    run("%s/bin/hdfs dfs -mkdir -p 'hdfs://%s:50040/%s'"
        % (conf['path'], env.master, dir)
    )

def copy_to_hdfs(src, dest):
    run("%s/bin/hdfs dfs -put -f -p '%s' 'hdfs://%s:50040/%s'"
        % (conf['path'], src, env.master, dest)
    )

def delete_from_hdfs(path):
     run("%s/bin/hdfs dfs -rm -r 'hdfs://%s:50040/%s'"
        % (conf['path'], env.master, path)
     )

def get_hdfs_address():
    return "hdfs://%s:50040/" % env.master

@task
@roles('master')
def run_jar(path, jar_name, args, clazz=None, upload=False):
    print "running %s with args: %s" % (jar_name, args)
    args = [str(a) for a in args]
    job_args = ' '.join(args)
    if upload:
        put("%s/%s" % (path, jar_name), conf['path'])
        path = conf['path']
    with cd(conf['path']):
        class_loader = "%s" % clazz if clazz else ""
        run("bin/hadoop jar %s/%s %s %s"
            % (path, jar_name, class_loader, job_args))