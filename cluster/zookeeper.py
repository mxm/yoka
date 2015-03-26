from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd
from maintenance import pull_from_master
from utils import process_template, get_slave_id


from configs import zookeeper_config as conf

# defaults to tmp but is set by actual system class
PATH = "/tmp/zookeeper"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % PATH)
    run("mkdir -p '%s'" % PATH)
    run("curl %s | tar xz -C %s" % (conf['source'], PATH))
    run("mv %s/zookeeper*/* %s" % (PATH, PATH))


@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(PATH, PATH)

@task
# configure on all hosts
def configure():
    # config
    context = conf.copy()
    context['server'] = [{'i': i, 'name': name} for i, name in enumerate(env.hosts)]
    destination = "%s/%s" % (PATH, "conf")
    process_template("zookeeper", "zoo.cfg.mustache", context, destination)
    # data dir
    run("rm -rf %s" % conf['data_dir'])
    run("mkdir -p %s" % conf['data_dir'])

@task
@parallel
def create_myid_file():
    myid = get_slave_id(env.host_string)
    run("echo %s > %s/myid" % (myid, conf['data_dir']))

@task
@parallel
def nodes(action="start"):
    with cd(PATH):
        run("./bin/zkServer.sh %s" % action)