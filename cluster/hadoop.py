from fabric.decorators import task, roles, parallel
from fabric.api import env, run, sudo, put, cd, execute
from maintenance import pull_from_master, set_java_home
from utils import process_template

from time import sleep

from configs import hadoop_config as conf

# defaults to home but is set by actual system class
PATH = "/tmp/hadoop"
namenode_dir = "hdfs-namenode"
datanode_dir = "hdfs-datanode"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % PATH)
    run("mkdir -p '%s'" % PATH)
    run("curl %s | tar xz -C %s" % (conf['source'], PATH))
    run("mv %s/hadoop*/* %s" % (PATH, PATH))

@roles('master')
def format_hdfs_master():
    run("rm -rf %s/%s" % (conf['data_path'], namenode_dir))
    run("mkdir -p %s/%s" % (conf['data_path'], namenode_dir))
    run("%s/bin/hdfs namenode -format" % PATH)

@task
@roles('slaves')
def delete_data_slaves():
    run("rm -rf %s/%s" % (conf['data_path'], datanode_dir))

@task
@roles('master')
def configure():
    set_java_home("%s/etc/hadoop/hadoop-env.sh" % PATH)
    # configure hdfs
    context = conf.copy()
    context['master'] = env.master
    context['namenode_path'] = "%s/%s" % (conf['data_path'], namenode_dir)
    context['datanode_path'] = "%s/%s" % (conf['data_path'], datanode_dir)
    destination = PATH + "/etc/hadoop/"
    process_template("hadoop", "hdfs-site.xml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context['slaves'] = slaves
    process_template("hadoop", "slaves.mustache", context, destination)
    # delete all data and ensure consistent state
    execute(format_hdfs_master)
    execute(delete_data_slaves)
    # configure YARN
    process_template("hadoop", "core-site.xml.mustache", context, destination)
    process_template("hadoop", "yarn-site.xml.mustache", context, destination)
    # configure MapReduce v2
    process_template("hadoop", "mapred-site.xml.mustache", context, destination)

@task
@parallel
def set_environment_variables(file="~/.profile"):
    run("echo export HADOOP_HOME='%s' >> %s" % (PATH, file))
    run("echo export HADOOP_COMMON_HOME='%s' >> %s" % (PATH, file))
    run("echo export HADOOP_CONF_DIR='%s/etc/hadoop' >> %s" % (PATH, file))
    run("echo export HADOOP_HDFS_HOME='%s' >> %s" % (PATH, file))
    run("echo export HADOOP_MAPRED_HOME='%s' >> %s" % (PATH, file))
    run("echo export HADOOP_YARN_HOME='%s' >> %s" % (PATH, file))

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(PATH, PATH)

@task
@roles('master')
def master(action="start"):
    run("%s/sbin/hadoop-daemon.sh --config %s/etc/hadoop/ --script hdfs %s namenode" % (PATH, PATH, action))
    run("%s/sbin/yarn-daemon.sh --config %s/etc/hadoop/ %s resourcemanager" % (PATH, PATH, action))
    run("%s/sbin/mr-jobhistory-daemon.sh --config %s/etc/hadoop/ %s historyserver" % (PATH, PATH, action))
    sleep(1)

@task
@roles('slaves')
@parallel
def slaves(action="start"):
    run("%s/sbin/hadoop-daemon.sh --config %s/etc/hadoop/ --script hdfs %s datanode" % (PATH, PATH, action))
    run("%s/sbin/yarn-daemon.sh --config %s/etc/hadoop/ %s nodemanager" % (PATH, PATH, action))

def mkdir_hdfs(dir):
    dir = dir if is_hdfs_address(dir) else "%s/%s" % (get_hdfs_address(), dir)
    run("%s/bin/hdfs dfs -mkdir -p '%s'"
        % (PATH, dir)
    )

def copy_to_hdfs(src, dest):
    dest = dest if is_hdfs_address(dest) else "%s/%s" % (get_hdfs_address(), dest)
    run("%s/bin/hdfs dfs -put -f -p '%s' '%s'"
        % (PATH, src, dest)
    )

def delete_from_hdfs(path):
    path = path if is_hdfs_address(path) else "%s/%s" % (get_hdfs_address(), path)
    run("%s/bin/hdfs dfs -rm -r '%s'"
    % (PATH, path)
    )

def is_hdfs_address(address):
    return address.startswith("hdfs://")

def get_hdfs_address():
    return "hdfs://%s:50040/" % env.master

@task
@roles('master')
def run_jar(path, jar_name, args, clazz=None, upload=False):
    print "running %s with args: %s" % (jar_name, args)
    args = [str(a) for a in args]
    job_args = ' '.join(args)
    if upload:
        put("%s/%s" % (path, jar_name), PATH)
        path = PATH
    with cd(path):
        class_loader = "%s" % clazz if clazz else ""
        run("bin/hadoop jar %s/%s %s %s"
            % (path, jar_name, class_loader, job_args))