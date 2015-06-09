from fabric.decorators import task, parallel, runs_once
from utils import LocalCommand, RemoteCommand
from getpass import getuser
import json
import pickle
import os

from fabric.api import env, run, sudo
from configs import local_cluster_config as conf

def get_slave_id_dict(master, slaves):
    # dictionary which gives each slave an id
    ids = {}
    max = 0
    for (slave_id, slave) in enumerate(slaves):
        ids[slave] = slave_id
        max = slave_id
    ids[master] = max + 1
    return ids

def init():
    env.hostnames = []
    env.hosts = []
    env.roles = {}
    master = conf['master']
    slaves = conf['slaves']
    if master and slaves:
        #global conf
        #conf = config.config
        # extract internal and external addresses
        master_internal, master_external = master
        slaves_internal, slaves_external = [list(addresses) for addresses in zip(*slaves)]
        # external addresses
        env.hosts = slaves_external + [master_external]
        # internal address
        env.master = master_internal
        # internal addresses
        env.slaves = slaves_internal
        # not needed for local mode
        env.hostnames = env.hosts
        #env.disknames = [name + "-disk" for name in hostnames]
        #env.host_dict = get_host_dict()
        env.roledefs = {'slaves' : slaves_external, 'master' : [master_external]}
        env.key_filename = conf['ssh_key']
        env.ids = get_slave_id_dict(master_external, slaves_external)
        env.keepalive = 60
        # at most 10 parallel executions
        env.pool_size = 10