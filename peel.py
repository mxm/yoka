from fabric.decorators import task, roles, runs_once

@task
@runs_once
def configure():
    pass

@task
@roles('master')
def deploy():
    put("", "")

@roles('master')
def start():
    pass

"""
system {
    default {
        config {
            slaves = ['localhost2']

system {
    flink {
        path {
            home = "/home/max/Dev/incubator-flink/flink-dist/target/flink-0.8-incubating-SNAPSHOT-bin/flink-0.8-incubating-SNAPSHOT/"
            config = ${system.flink.path.home}"/conf"
            log = ${system.flink.path.home}"/log"
        }
        config {
            # put list of slaves
    ---->   slaves = ${system.default.config.slaves}
            # flink.yaml entries
            yaml {
                env.java.home = ${system.default.config.java.home}
                taskmanager.numberOfTaskSlots = ${system.default.config.parallelism.per-node}
    ---->       jobmanager.rpc.address = "bla"
    with cd(conf.PEEL_CONFIG_DIR):
        pass
    pass
"""
