from fabric.decorators import task, parallel, runs_once
from utils import LocalCommand, RemoteCommand
from getpass import getuser
import json
import pickle
import os

from fabric.api import env, run, sudo
from configs import local_cluster_config as conf

def get_slave_id_dict():
    # dictionary which gives each slave an id
    ids = {}
    ids[conf['master']] = 0
    for (slave_id, slave) in enumerate(conf['slaves']):
        ids[slave] = slave_id+1
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
        env.hosts = slaves + [master]
        env.master = master
        env.slaves = slaves
        env.hostnames = env.hosts
        #env.disknames = [name + "-disk" for name in hostnames]
        #env.host_dict = get_host_dict()
        env.roledefs = {'slaves' : slaves, 'master' : [master]}
        env.key_filename = conf['ssh_key']
        env.ids = get_slave_id_dict()
        env.keepalive = 60