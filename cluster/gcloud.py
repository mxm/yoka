from fabric.decorators import task, parallel, runs_once
from core.utils import Prompt
from utils import LocalCommand, RemoteCommand
from getpass import getuser
import json
import pickle
import os

from fabric.api import env, run, sudo
from configs import compute_engine_config as conf

import core.log
logger = core.log.get_logger()

class ExistingInstancesException(Exception):
    pass

config_file = "gcloud_conf.data"

class Configuration(object):

    def __init__(self, config, master, slaves):
        self.config = config
        self.master = master
        self.slaves = slaves

    def get_master_ip(self):
        try:
            return self.master[1]
        except:
            return None

    def get_master_name(self):
        try:
            return self.master[0]
        except:
            return None

    def get_slave_ips(self):
        return self.slaves.values()

    def get_slave_names(self):
        return self.slaves.keys()

    def get_ips(self):
        master_ip = self.get_master_ip()
        ips = self.get_slave_ips()
        ips.append(master_ip)
        return ips if master_ip else None

    def get_hostnames(self):
        master_name = self.get_master_name()
        names = self.get_slave_names()
        names.append(master_name)
        return names if master_name else None

    def get_host_dict(self):
        # map ips to host names
        host_dict = {ip: name for name, ip in self.slaves.iteritems()}
        host_dict[self.get_master_ip()] = self.get_master_name()
        return host_dict

    def get_id_dict(self):
        # dictionary which gives each slave an id
        max = 0
        ids = {}
        for (slave_id, slave) in enumerate(self.get_slave_ips()):
            ids[slave] = slave_id
            max = slave_id
        ids[self.get_master_ip()] = max + 1
        return ids

    def save(self):
        with open(config_file, 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def __str__(self):
        return str(self.get_ips())

    @staticmethod
    def load():
        try:
            with open(config_file, 'rb') as input:
                config = pickle.load(input)
                return config
        except:
            return None

    @staticmethod
    def delete():
        try:
            os.remove(config_file)
        except OSError:
            logger.exception("Failed to delete gcloud config file!")


@task
@runs_once
def install_gcloud():
    pass

@task
@runs_once
def authenticate():
    LocalCommand(
        "gcloud auth login",
        "--project %s" % conf['project_name']
    ).execute()

@task
@runs_once
def configure_ssh():
    LocalCommand(
        "gcloud compute config-ssh",
        "--project %s" % conf['project_name']
    ).execute()

@task
@runs_once
def create_instances():
    if env.hostnames:
        print "Old instancecs exist. Not creating new instances."
        raise ExistingInstancesException("Old instances exist.")
    logger.info("Creating machines.")
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "instances create %s" % ' '.join(get_hostnames()),
        "--zone %s" % conf['zone'],
        "--machine-type %s" % conf['machine_type'],
        "--network 'default'",
        "--maintenance-policy 'MIGRATE'",
        "--scopes 'https://www.googleapis.com/auth/devstorage.read_only'",
        "--image '%s'" % conf['disk_image'],
        "--boot-disk-type %s" % conf['disk_type'],
        "-q"
    ).execute()
    logger.info("Creating disks.")
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "disks create %s" % ' '.join(get_disknames()),
        "--zone %s" % conf['zone'],
        "--size %s" % conf['disk_space_gb'],
        "--type %s" % conf['disk_type'],
        "-q"
    ).execute()
    Configuration.delete()
    init()

@task
@parallel
def attach_disk():
    host_name = env.host_dict[env.host_string]
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "instances attach-disk %s" % host_name,
        "--zone %s" % conf['zone'],
        "--device-name additional-storage",
        "--disk %s-disk" % host_name,
        "-q"
    ).execute()

@task
@parallel
def mount_disk():
    # create partition table and partition
    sudo('echo -e "o\nn\np\n1\n\n\n\n\n\nw" | fdisk /dev/disk/by-id/google-additional-storage')
    # format partition
    sudo('mkfs -t ext4 /dev/disk/by-id/google-additional-storage-part1 >/dev/null')
    run("mkdir -p %s" % conf['disk_mount_path'])
    # mount
    sudo("mount -t ext4 /dev/disk/by-id/google-additional-storage-part1 %s" % conf['disk_mount_path'])
    user = getuser()
    sudo("chown %s:%s %s" % (user, user, conf['disk_mount_path']))

@task
@runs_once
def delete_instances(prompt=False):
    if not env.hostnames:
        print "No hostnames configured. Cannot delete instances."
        return
    if prompt:
        ask = Prompt("Delete cluster with the following hostnames? (y/n) %s" % env.hostnames, "y")
        if not ask.prompt():
            return
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "instances delete %s" % ' '.join(env.hostnames),
        "--zone %s" % conf['zone'],
        "-q"
    ).execute()
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "disks delete %s" % ' '.join(env.disknames),
        "--zone %s" % conf['zone'],
        "-q"
    ).execute()
    Configuration.delete()
    init()

@task
def create_config():
    output = LocalCommand(
        "gcloud compute instances list",
        "--project %s" % conf['project_name'],
        "--regexp '%s.*'" % conf['prefix'],
        "--format json"
    ).execute(capture=True)
    data = json.loads(output)
    master = ()
    slaves = {}
    for machine in data:
        name = machine[u'name']
        ip = machine[u'networkInterfaces'][0][u'accessConfigs'][0][u'natIP']
        if name == get_master_hostname():
            master = (name, ip)
        else:
            slaves[name] = ip
    config = Configuration(conf, master, slaves)
    config.save()
    return Configuration.load()

def get_master_hostname():
    return conf['prefix'] + "master"

def get_hostnames():
    workers = [conf['prefix'] + "worker" + str(x) for x in xrange(0, conf['num_workers'])]
    workers.append(get_master_hostname())
    return workers

def get_disknames():
    return [name + "-disk" for name in get_hostnames()]

def init():
    env.hostnames = []
    env.hosts = []
    env.roles = {}
    config = Configuration.load()
    # apply old config
    if config:
        globals()['conf'] = config.config
    else:
        config = create_config()
    ips = config.get_ips()
    hostnames = config.get_hostnames()
    master_ip = config.get_master_ip()
    slave_ips = config.get_slave_ips()
    if ips and hostnames:
        env.hosts = ips
        env.master = config.get_master_name()
        env.slaves = config.get_slave_names()
        env.hostnames = hostnames
        env.disknames = [name + "-disk" for name in hostnames]
        env.host_dict = config.get_host_dict()
        env.roledefs = {'slaves' : slave_ips, 'master' : [master_ip]}
        env.key_filename = "~/.ssh/google_compute_engine"
        env.ids = config.get_id_dict()
        env.keepalive = 60
        # at most 10 parallel executions, otherwise this can lead to failures
        env.pool_size = 10
