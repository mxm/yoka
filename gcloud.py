from fabric.decorators import task, roles, runs_once
import config as conf
from utils import LocalCommand
import json
import pickle
import os

from fabric.api import env


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
    local("gcloud config set project %s" % conf.GCE_PROJECT_NAME)
    local("gcloud auth login")

    setup()

@task
@runs_once
def create_instances():
    if env.hostnames:
        print "Old instancecs exist. Not creating new instances."
        return
    LocalCommand(
        "gcloud compute --project %s" % conf.GCE_PROJECT_NAME,
        "instances create %s" % ' '.join(get_hostnames()),
        "--zone %s" % conf.GCE_ZONE,
        "--machine-type %s" % conf.GCE_MACHINE_TYPE,
        "--network 'default'",
        "--maintenance-policy 'MIGRATE'",
        "--scopes 'https://www.googleapis.com/auth/devstorage.read_only'",
        "--image '%s'" % conf.GCE_DISK_IMAGE,
        "--boot-disk-type 'pd-standard'",
        "-q"
    ).execute()
    create_config()


@task
@runs_once
def delete_instances():
    if not env.hostnames:
        print "No hostnames configured. Cannot delete instances."
        return
    LocalCommand(
        "gcloud compute --project %s" % conf.GCE_PROJECT_NAME,
        "instances delete %s" % ' '.join(env.hostnames),
        "--zone %s" % conf.GCE_ZONE,
        "-q"
    ).execute()
    Configuration.delete()

@task
@runs_once
def create_config():
    output = LocalCommand(
        "gcloud compute instances list",
        "--regexp '%s.*'" % conf.GCE_MACHINE_PREFIX,
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
    return conf.GCE_MACHINE_PREFIX + "master"

def get_hostnames():
    workers = [conf.GCE_MACHINE_PREFIX + "worker" + str(x) for x in xrange(0, conf.NUM_WORKERS)]
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
    if ips and hostnames and master and slaves:
        env.hosts = ips
        env.master = config.get_master_name()
        env.slaves = config.get_slaves_names()
        env.hostnames = hostnames
        env.roledefs = {'slaves' : slaves, 'master' : [master]}
        env.key_filename = "~/.ssh/google_compute_engine"

init()
