from fabric.decorators import task, roles, parallel, runs_once
from fabric.api import sudo, run, execute, env
import config as conf

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
    sudo("apt-get -y install %s" % package)

@task
@parallel
def install_dependencies():
    update_package_cache()
    install("openjdk-7-jdk")
    install("maven")
    install("git")
    install("curl")

@task
@parallel
def set_java_home(file="~/.bashrc"):
    java_home = conf.JAVA_HOME
    run("echo 'export JAVA_HOME=%s' >> %s" % (java_home, file))
    sudo("echo 'export JAVA_HOME=%s' >> %s" % (java_home, "/root/.bashrc"))

@task
@runs_once
@roles('master')
def generate_key():
    run("yes | ssh-keygen -q -t rsa -f ~/.ssh/id_rsa -N ''")
    return (run("cat ~/.ssh/id_rsa.pub", quiet=True), run("cat ~/.ssh/id_rsa", quiet=True))

@task
def set_key():
    (publickey, privatekey) = execute(generate_key, role='master').values()[0]
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
def pull_from_master(path):
    run("scp -r %s:'%s' ~" % (env.master, path), quiet=False)
