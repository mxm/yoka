from fabric.decorators import task, roles

@task
@roles('master')
def install_dependencies():
    run("git clone %s flink" % conf.FLINK_REPOSITORY)
    with cd("flink"):
        run("git checkout %s" % conf.FLINK_COMMIT)
        run("mvn clean install -DskipTests")

@task
@roles('master')
def start_master():
    pass

@task
@roles('slaves')
def start_slaves():
    pass
