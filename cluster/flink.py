from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd, put, quiet
from maintenance import pull_from_master, find_java_home
from utils import process_template, copy_log, get_slave_id

from time import sleep

from configs import flink_config as conf

# defaults to home but is set by actual system class
PATH = "~"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % PATH)
    run("git clone %s %s" % (conf['git_repository'], PATH))

@task
@roles('master')
def get_flink_dist_path(yarn=False):
    if yarn:
        return run("cd %s/flink-dist/target/flink*/flink-yarn*/;pwd" % PATH)
    else:
        return run("cd %s/flink-dist/target/flink*/flink*/;pwd" % PATH)

@task
@roles('master')
def configure():
    with cd(PATH):
        run("git checkout %s" % conf['git_commit'])
        run("echo commit: ")
        run("git rev-parse HEAD")
        run("mvn clean install -DskipTests > build.log")
    context = conf.copy()
    context['java_home'] = find_java_home()
    context['master'] = env.master
    # get hadoop conf environment variable
    context['hadoop_conf_path'] = run("echo $HADOOP_CONF_DIR")
    destination = get_flink_dist_path() + "/conf"
    process_template("flink", "flink-conf.yaml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("flink", "slaves.mustache", context2, destination)

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(PATH, PATH)

@task
@roles('slaves')
@parallel
def create_temp_dir():
    temp_dirs = conf["taskmanager_temp_dirs"]
    if temp_dirs:
        for dir in temp_dirs.split(":"):
            run("mkdir -p '%s'" % dir)

@task
@roles('master')
def master(action="start", yarn=False):
    path = get_flink_dist_path(yarn=yarn)
    with cd(path):
        if not yarn:
            run('nohup bash bin/jobmanager.sh %s cluster' % action)
        else:
            if action == 'start':
                # the sleep is necessaray
                # see https://github.com/fabric/fabric/issues/1158
                run("nohup bin/yarn-session.sh -n %d & sleep 1" % conf['num_task_slots'])
            elif action == 'stop':
                run("kill -s SIGINT `jps | grep FlinkYarn | cut -d' ' -f1`")
    sleep(10)

@task
@roles('slaves')
@parallel
def slaves(action="start", yarn=False):
    path = get_flink_dist_path(yarn=yarn)
    with cd(path):
        if not yarn:
            run('nohup bash bin/taskmanager.sh %s' % action)
    sleep(1)

@task
@roles('master')
def run_jar(path, jar_name, args, dop=None, clazz=None, upload=False):
    print "running %s with args: %s" % (jar_name, args)
    args = [str(a) for a in args]
    job_args = ' '.join(args)
    if upload:
        put("%s/%s" % (path, jar_name), PATH)
        path = PATH
    # figure out if we're running Flink on YARN
    with quiet():
        if run("jps | grep Yarn").failed:
            yarn = False
        else:
            yarn = True
    with cd(get_flink_dist_path(yarn)):
        class_loader = "-c '%s'" % clazz if clazz else ""
        dop = dop if dop else get_degree_of_parallelism()
        run("bin/flink run -v -p %d %s %s/%s %s"
            % (dop, class_loader, path, jar_name, job_args))

@task
@roles('master')
def copy_log_master(dest_path, yarn=False):
    path = get_flink_dist_path(yarn) + "/log"
    # flink bug FLINK-1361: either jobmanager or jobManager
    log_file = "flink-*-job[Mm]anager-*"
    for extension in ["log", "out"]:
        copy_log("%s/%s.%s" % (path, log_file, extension),
                 "%s/flink_jobmanager.%s" % (dest_path, extension)
        )

@task
@roles('slaves')
def copy_log_slaves(dest_path, yarn=False):
    path = get_flink_dist_path(yarn) + "/log"
    log_file = "flink-*-taskmanager-*"
    for extension in ["log", "out"]:
        copy_log("%s/%s.%s" % (path, log_file, extension),
                 "%s/flink_taskmanager_%s.%s" % (dest_path, get_slave_id(env.host_string), extension)
        )

def get_degree_of_parallelism():
    #return conf['num_workers'] * conf['num_cores']
    return conf['parallelization']