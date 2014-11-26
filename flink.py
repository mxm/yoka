from __future__ import with_statement
from fabric.decorators import task, roles
from fabric.api import env, run, cd
import config as conf
from maintenance import pull_from_master
from utils import process_template

@task
@roles('master')
def install_dependencies():
    run("git clone %s %s" % (conf.FLINK_REPOSITORY, conf.FLINK_PATH))
    with cd("flink"):
        run("git checkout %s" % conf.FLINK_COMMIT)
        run("mvn clean install -DskipTests")

@task
@roles('master')
def configure_flink():
    context = {
        'master' : env.master,
        'java_home' : conf.JAVA_HOME,
        'number_taskslots' : conf.FLINK_NUMBER_TASK_SLOTS,
        'parallelization' : conf.FLINK_PARALLELIZATION,
    }
    destination = run("cd %s/flink-dist/target/flink*/flink*/conf;pwd" % conf.FLINK_PATH)
    print destination
    process_template("flink", "flink-conf.yaml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("flink", "slaves.mustache", context2, destination)


@task
@roles('slaves')
def pull_flink():
    pull_from_master(conf.FLINK_PATH)

@task
@roles('master')
def flink_master(action="start"):
    path = run("cd %s/flink-dist/target/flink*/flink*/;pwd" % conf.FLINK_PATH)
    with cd(path):
        run('nohup bash bin/jobmanager.sh %s cluster' % action)

@task
@roles('slaves')
def flink_slaves(action="start"):
    path = run("cd %s/flink-dist/target/flink*/flink*/;pwd" % conf.FLINK_PATH)
    with cd(path):
        run('nohup bash bin/taskmanager.sh %s' % action)
