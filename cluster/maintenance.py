from fabric.decorators import task, roles, parallel, runs_once
from fabric.api import sudo, run, execute, env

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
    run("mkdir -p '%s'" % dir)

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