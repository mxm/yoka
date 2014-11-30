from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd, put
import config as conf
from maintenance import pull_from_master
from utils import process_template

@task
@roles('master')
def install():
    run("git clone %s %s" % (conf.FLINK_REPOSITORY, conf.FLINK_PATH))
    with cd("flink"):
        run("git checkout %s" % conf.FLINK_COMMIT)
        run("mvn clean install -DskipTests")

def get_flink_dist_path():
    return run("cd %s/flink-dist/target/flink*/flink*/;pwd" % conf.FLINK_PATH)

@task
@roles('master')
def configure():
    context = {
        'master' : env.master,
        'java_home' : conf.JAVA_HOME,
        'number_taskslots' : conf.FLINK_NUMBER_TASK_SLOTS,
        'parallelization' : conf.FLINK_PARALLELIZATION,
        'jobmanager_heap' : conf.FLINK_JOBMANAGER_HEAP,
        'taskmanager_heap' : conf.FLINK_PARALLELIZATION,
        'taskmanager_num_buffers' : conf.FLINK_NUMBER_TASK_SLOTS,
    }
    destination = get_flink_dist_path() + "/conf"
    process_template("flink", "flink-conf.yaml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("flink", "slaves.mustache", context2, destination)


@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(conf.FLINK_PATH)

@task
@roles('master')
def master(action="start"):
    path = get_flink_dist_path()
    with cd(path):
        run('nohup bash bin/jobmanager.sh %s cluster' % action)

@task
@roles('slaves')
@parallel
def slaves(action="start"):
    path = get_flink_dist_path()
    with cd(path):
        run('nohup bash bin/taskmanager.sh %s' % action)

@task
@roles('master')
def run_jar(path, jar_name, args):
    print "running %s with args: %s" % (jar_name, args)
    put("%s/%s" % (path, jar_name), conf.FLINK_PATH)
    with cd(get_flink_dist_path()):
        job_args = ' '.join(args)
        run("bin/flink run -v '%s/%s' %s"
            % ( conf.FLINK_PATH, jar_name, job_args))
