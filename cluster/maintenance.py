from fabric.decorators import task, roles, parallel, runs_once
from fabric.api import sudo, run, execute, env, settings

from time import sleep

import core.log

logger = core.log.get_logger()


@task
@parallel
def update_package_cache():
    sudo("apt-get update > /dev/null")

@task
@parallel
def upgrade():
    sudo("apt-get -y upgrade")

@task
@parallel
def install(package):
    sudo("apt-get -y install %s > /dev/null" % package)

@task
@parallel
def install_dependencies():
    install("openjdk-7-jdk")
    install("maven")
    install("git")
    install("curl")

def find_java_home():
    return run("readlink -f /usr/bin/javac | sed 's:bin/javac::'")

@task
@parallel
def set_java_home(file="~/.bashrc"):
    java_home = find_java_home()
    run("echo 'export JAVA_HOME=%s' >> %s" % (java_home, file))
    with settings(warn_only=True):
        sudo("echo 'export JAVA_HOME=%s' >> %s" % (java_home, file))
        sudo("echo 'export HADOOP_SECURE_DN_USER=max' >> %s" % file)
        sudo("echo 'export JSVC_HOME=/usr/bin' >> %s" % file)
        
@task
@roles('master')
@runs_once
def generate_key():
    run("yes | ssh-keygen -q -t rsa -f ~/.ssh/yoka -N ''")
    return (run("cat ~/.ssh/yoka.pub", quiet=False), run("cat ~/.ssh/yoka", quiet=True))

@task
# calls runs_once method, not safe to be parallel
#@parallel
def set_key():
    (publickey, privatekey) = execute(generate_key).values()[0]
    run("echo '%s' > ~/.ssh/yoka.pub" % publickey, quiet=False)
    run("echo '%s' > ~/.ssh/yoka" % privatekey, quiet=True)
    run("chmod 700 ~/.ssh/yoka")
    run("echo '%s' >> ~/.ssh/authorized_keys" % publickey, quiet=True)
    ssh_config = ""
    for host in env.hostnames:
        ssh_config += """
            Host %s
            HostName %s
            IdentityFile ~/.ssh/yoka
            UserKnownHostsFile=/dev/null
            CheckHostIP=no
            StrictHostKeyChecking=no
        """ % (host, host)
    run("echo '%s' >> ~/.ssh/config" % ssh_config)

@task
def set_up_dir(dir):
    # for safety, check if this directory does not exist
    # or delete, if it contains a yoka lock file
    with settings(warn_only=True):
        if run("[ -d '%s' ]" % dir).failed:
            logger.info("Creating working directory %s" % dir)
            run("mkdir -p '%s'" % dir)
            run("touch '%s/yoka.dir.lock'" % dir)
        elif run("[ -f '%s/yoka.dir.lock' ]" % dir).succeeded:
            logger.info("Removing working directory %s" % dir)
            sudo("rm -rf '%s'" % dir)
            run("rm -rf '%s'" % dir)
            set_up_dir(dir)
        else:
            logger.error("Working directory exists but has not been used as a Yoka directory before.\n \
            Deleting it would be unsafe. Please delete manually! Exiting.\nDirectory: %s" % dir)
            raise Exception("Could not create working dir: %s" % dir)

@task
# this fails if too many hosts pull at once
#@parallel(pool_size=10)
# now set globally through environment dictionary fabric.env
@parallel
@roles('slaves')
def pull_from_master(path, dest="~"):
    """
    :param path: dir to be copied
    :param dest: destination of the dir
    :return:
    """
    for i in range(0, 5):
        try:
            run("rsync -aP %s:%s/ %s > /dev/null" % (env.master, path, dest))
            break
        except:
            logger.warn("Failed to execute rsync for host %s" % env.host_string)
            sleep(5)