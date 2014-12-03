from lib import Cluster

from cluster import gcloud, maintenance

from fabric.api import execute

class ComputeEngine(Cluster):

    def setup(self):
        gcloud.conf = self.config
        execute(gcloud.init)
        execute(gcloud.create_instances)
        #maintenance.upgrade()
        execute(maintenance.install_dependencies)
        execute(maintenance.set_java_home)
        execute(maintenance.set_key)

    def shutdown(self):
        execute(gcloud.delete_instances)