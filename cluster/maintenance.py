from fabric.decorators import task, roles, parallel, runs_once
from fabric.api import sudo, run, execute, env


@task
@parallel
def update_package_cache():
    sudo("apt-get update")

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
    update_package_cache()
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
    sudo("echo 'export JAVA_HOME=%s' >> %s" % (java_home, "/root/.bashrc"))

@task
@runs_once
@roles('master')
def generate_key():
    run("yes | ssh-keygen -q -t rsa -f ~/.ssh/id_rsa -N ''")
    return (run("cat ~/.ssh/id_rsa.pub", quiet=True), run("cat ~/.ssh/id_rsa", quiet=True))

@task
@parallel
def set_key():
    (publickey, privatekey) = execute(generate_key).values()[0]
    run("echo '%s' > ~/.ssh/id_rsa.pub" % publickey, quiet=True)
    run("echo '%s' > ~/.ssh/id_rsa" % privatekey, quiet=True)
    run("chmod 700 ~/.ssh/id_rsa")
    run("echo '%s' >> ~/.ssh/authorized_keys" % publickey, quiet=True)
    ssh_config = """Host *
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null"""
    run("echo '%s' > ~/.ssh/config" % ssh_config)


@task
@parallel
@roles('slaves')
def pull_from_master(path, dest="~"):
    run("rsync -a --progress --exclude 'logs' %s:'%s' %s" % (env.master, path, dest), quiet=False)
