from fabric.decorators import task, roles, parallel
from fabric.api import env, run, sudo, put, cd
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
    # configure hdfs
    context =  conf.copy()
    context['master'] = env.master
    destination = conf['path'] + "/" + conf['config_path']
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
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s namenode" % (conf['path'], conf['path'], conf['config_path'], action))
    sudo("%s/sbin/yarn-daemon.sh --config %s/%s %s resourcemanager" % (conf['path'], conf['path'], conf['config_path'], action))
    sudo("%s/sbin/mr-jobhistory-daemon.sh --config %s/%s %s historyserver" % (conf['path'], conf['path'], conf['config_path'], action))

@task
@roles('slaves')
@parallel
def slaves(action="start"):
    sudo("%s/sbin/hadoop-daemon.sh --config %s/%s --script hdfs %s datanode" % (conf['path'], conf['path'], conf['config_path'], action))
    sudo("%s/sbin/yarn-daemon.sh --config %s/%s %s nodemanager" % (conf['path'], conf['path'], conf['config_path'], action))

def mkdir_hdfs(dir):
    run("%s/bin/hdfs dfs -mkdir -p 'hdfs://%s:50040/%s'"
        % (conf['path'], env.master, dir)
    )

def copy_to_hdfs(src, dest):
    run("%s/bin/hdfs dfs -put -f -p '%s' 'hdfs://%s:50040/%s'"
        % (conf['path'], src, env.master, dest)
    )

def delete_from_hdfs(path):
     run("%s/bin/hdfs dfs -rm 'hdfs://%s:50040/%s'"
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