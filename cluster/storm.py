from __future__ import with_statement
from fabric.decorators import task, roles, parallel
from fabric.api import env, run, cd
from maintenance import pull_from_master
from utils import process_template, get_slave_id


from configs import storm_config as conf

# defaults to tmp but is set by actual system class
PATH = "/tmp/storm"

@task
@roles('master')
def install():
    run("rm -rf '%s'" % PATH)
    run("mkdir -p '%s'" % PATH)
    run("curl %s | tar xz -C %s" % (conf['binaries'], PATH))
    run("mv %s/apache-storm*/* %s" % (PATH, PATH))

@task
@roles('master')
def configure():
    # config
    context = conf.copy()
    context['zookeeper'] = [{'server': address} for address in env.hostnames[0:conf['num_zookeeper_instances']]]
    context['master'] = env.master
    context['supervisor_slots'] = [{'slot': 6700 + i} for i in range(conf['num_supervisor_slots'])]
    destination = "%s/%s" % (PATH, "conf")
    process_template("storm", "storm.yaml.mustache", context, destination)


@task
@roles('slaves')
@parallel
def pull():
    pull_from_master(PATH, PATH)


@task
@parallel
def create_local_dir():
    run("rm -rf %s" % conf['local_dir'])
    run("mkdir -p %s" % conf['local_dir'])

@task
@roles('master')
def master():
    with cd(PATH):
        # the sleep is necessaray
        # see https://github.com/fabric/fabric/issues/1158
        run("nohup ./bin/storm nimbus & sleep 1")
        run("nohup ./bin/storm ui & sleep 1")

@task
@roles('slaves')
@parallel
def slaves():
    with cd(PATH):
        run("nohup ./bin/storm supervisor & sleep 1")