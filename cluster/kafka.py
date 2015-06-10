from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd
from maintenance import pull_from_master
from utils import process_template, get_slave_id


from configs import kafka_config as conf

# defaults to tmp but is set by actual system class
PATH = "/tmp/kafka"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % PATH)
    run("mkdir -p '%s'" % PATH)
    run("curl %s | tar xz -C %s" % (conf['source'], PATH))
    run("mv %s/kafka*/* %s" % (PATH, PATH))


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
    context['id'] = get_slave_id(env.host_string)
    context['zookeeper_server'] = [{'name' : name} for name in env.hostnames[0:conf['num_instances']]]
    destination = "%s/%s" % (PATH, "config")
    process_template("kafka", "server.properties.mustache", context, destination)

@task
@parallel
def nodes(action="start"):
    with cd(PATH):
        run("nohup ./bin/kafka-server-%s.sh config/server.properties" % action)