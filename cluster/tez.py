from fabric.decorators import task, roles, parallel
from fabric.api import env, run, sudo, cd

from maintenance import install as install_pkg
from utils import process_template
from core.utils import GitRepository
from hadoop import copy_to_hdfs, mkdir_hdfs

from configs import tez_config as conf

# based on http://tez.apache.org/install.html

@task
@roles('master')
def install():
    # check out via GIT
    repo = GitRepository(conf['git_repository'], conf['path'])
    repo.clone()
    repo.checkout(conf['git_commit'])
    # protobuf compiler is a required (but not mentioned) prerequisite
    install_pkg("protobuf-compiler")
    install_pkg("bzip2")
    # modify pom.xml to build on debian
    with cd(conf['path']):
        run("sed -i 's/<protobuf.version>2.5.0<\/protobuf.version>/<protobuf.version>2.4.1<\/protobuf.version>/' pom.xml")

@task
@roles('master')
def get_tez_tarball_path(file):
    return run("ls %s/tez-dist/target/%s" % (conf['path'], file))

@task
@roles('master')
def configure():
    # build tez
    with cd(conf['path']):
        run("mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true > /dev/null")
    # copy to HDFS
    mkdir_hdfs("/tez")
    hdfs_path = "/tez/tarball.tar.gz"
    copy_to_hdfs(get_tez_tarball_path("tez-*SNAPSHOT.tar.gz"), hdfs_path)
    # configure tez
    destination = conf['path']
    process_template(module="tez",
                     template="tez-site.xml.mustache",
                     context={'hdfs_path' : hdfs_path},
                     destination=destination)
    # configure client
    sudo("rm -rf '%s'" % conf['path_client'])
    tarball_location = get_tez_tarball_path("tez*-minimal.tar.gz")
    run("mkdir -p %s" % conf['path_client'])
    run("tar -xzf %s -C %s" % (tarball_location, conf['path_client']))
    run("echo 'export TEZ_CONF_DIR=%s' >> ~/.bashrc" % conf['path'])
    run("echo 'export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*' >> ~/.bashrc")
