from __future__ import with_statement
from fabric.decorators import task
from fabric.api import execute, env

from cluster.utils import get_slave_id

from cluster import gcloud, maintenance, hadoop, flink, tez, utils
