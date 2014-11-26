from __future__ import with_statement
from fabric.api import env, local, settings, abort, run, cd
from fabric.contrib.console import confirm
from fabric.decorators import task, roles, runs_once, parallel

from utils import LocalCommand, RemoteCommand

import gcloud
import maintenance
import hadoop
import flink
#import peel

def all():
    paas_authenticate()
    paas_create_instances()
    cluster_install_dependencies()
    create_peel_config()
    deploy_peel()
    start_peel()
    get_results()
    paas_delete_instances()
