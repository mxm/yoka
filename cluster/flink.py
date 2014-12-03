from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd, put, sudo
from maintenance import pull_from_master, find_java_home
from utils import process_template

from configs import flink_config as conf

@task
@roles('master')
def install():
    sudo("rm -rf '%s'" % conf['path'])
    run("git clone %s %s" % (conf['git_repository'], conf['path']))

def get_flink_dist_path():
    return run("cd %s/flink-dist/target/flink*/flink*/;pwd" % conf['path'])

@task
@roles('master')
def configure():
    with cd("flink"):
        run("git checkout %s" % conf['git_commit'])
        run("mvn install -DskipTests")
    context = conf
    context['java_home'] = find_java_home()
    context['master'] = env.master
    destination = get_flink_dist_path() + "/conf"
    process_template("flink", "flink-conf.yaml.mustache", context, destination)
    slaves = '\n'.join(env.slaves)
    context2 = {'slaves' : slaves}
    process_template("flink", "slaves.mustache", context2, destination)

@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(conf['path'])

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
def run_jar(path, jar_name, args, clazz=None, upload=False):
    print "running %s with args: %s" % (jar_name, args)
    job_args = ' '.join(args)
    if upload:
        put("%s/%s" % (path, jar_name), conf['path'])
        path = conf['path']
    with cd(get_flink_dist_path()):
        class_loader = "-c '%s'" % clazz if clazz else ""
        run("bin/flink run -v %s '%s/%s' %s"
            % (class_loader, path, jar_name, job_args))
