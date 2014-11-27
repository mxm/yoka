from __future__ import with_statement
from fabric.api import env, local, settings, abort, run, cd, execute
from fabric.contrib.console import confirm
from fabric.decorators import task, roles, runs_once, parallel

from utils import LocalCommand, RemoteCommand

import gcloud
import maintenance
import hadoop
import flink
#import peel

execution = [
    gcloud.create_instances,
    maintenance.update_package_cache,
    maintenance.install_dependencies,
    maintenance.set_java_home,
    maintenance.set_key,
    hadoop.install,
    hadoop.configure,
    hadoop.pull,
    hadoop.master,
    hadoop.slaves,
    flink.install,
    flink.configure,
    flink.pull,
    flink.master,
    flink.slaves,
]

def setup_flink():
    for step in execution:
        print "executing", step.__name__
        execute(step)

def all():
    paas_authenticate()
    paas_create_instances()
    cluster_install_dependencies()
    create_peel_config()
    deploy_peel()
    start_peel()
    get_results()
    paas_delete_instances()
