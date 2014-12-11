from fabric.decorators import task, roles, runs_once
from utils import LocalCommand
import json
import pickle
import os

from fabric.api import env, local
from configs import compute_engine_config as conf


gcloud_file = "gcloud_conf.data"

class Configuration(object):

    def __init__(self, master, slaves):
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

    def get_slaves_ips(self):
        return self.slaves.values()

    def get_slaves_names(self):
        return self.slaves.keys()

    def get_ips(self):
        ips = self.get_slaves_ips()
        ips.append(self.get_master_ip())
        return ips

    def get_hostnames(self):
        names = self.get_slaves_names()
        names.append(self.get_master_name())
        return names

    def save(self):
        with open(gcloud_file, 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def __str__(self):
        return str(self.get_ips())

    @staticmethod
    def load():
        try:
            with open(gcloud_file, 'rb') as input:
                config = pickle.load(input)
                return config
        except:
            return None

    @staticmethod
    def delete():
        os.remove(gcloud_file)


@task
@runs_once
def install_gcloud():
    pass

@task
@runs_once
def authenticate():
    local("gcloud config set project %s" % conf['project_name'])
    local("gcloud auth login")

@task
@runs_once
def configure_ssh():
    local("gcloud compute config-ssh --project %s" % conf['project_name'])

@task
@runs_once
def create_instances():
    if env.hostnames:
        print "Old instancecs exist. Not creating new instances."
        return
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "instances create %s" % ' '.join(get_hostnames()),
        "--zone %s" % conf['zone'],
        "--machine-type %s" % conf['machine_type'],
        "--network 'default'",
        "--maintenance-policy 'MIGRATE'",
        "--scopes 'https://www.googleapis.com/auth/devstorage.read_only'",
        "--image '%s'" % conf['disk_image'],
        "--boot-disk-type 'pd-standard'",
        "--boot-disk-size %s" % conf['disk_space_gb'],
        "-q"
    ).execute()
    Configuration.delete()
    init()

@task
@runs_once
def delete_instances():
    if not env.hostnames:
        print "No hostnames configured. Cannot delete instances."
        return
    LocalCommand(
        "gcloud compute --project %s" % conf['project_name'],
        "instances delete %s" % ' '.join(env.hostnames),
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
    config = Configuration(master, slaves)
    config.save()
    return Configuration.load()

def get_master_hostname():
    return conf['prefix'] + "master"

def get_hostnames():
    workers = [conf['prefix'] + "worker" + str(x) for x in xrange(0, conf['num_workers'])]
    workers.append(get_master_hostname())
    return workers

def init():
    env.hostnames = []
    env.hosts = []
    env.roles = {}
    config = Configuration.load()
    if not config:
        config = create_config()
    ips = config.get_ips()
    hostnames = config.get_hostnames()
    master = config.get_master_ip()
    slaves = config.get_slaves_ips()
    # dictionary which gives each slave an id
    ids = {}
    ids[master] = 0
    for (slave_id, slave) in enumerate(slaves):
        ids[slave] = slave_id+1
    if ips and hostnames and master and slaves:
        env.hosts = ips
        env.master = config.get_master_name()
        env.slaves = config.get_slaves_names()
        env.hostnames = hostnames
        env.roledefs = {'slaves' : slaves, 'master' : [master]}
        env.key_filename = "~/.ssh/google_compute_engine"
        env.ids = ids
        env.keepalive = 60