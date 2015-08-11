from lib import Cluster, ResumeMode

from cluster import gcloud, local, maintenance

from core.utils import Prompt, MultiPrompt

from fabric.api import execute

import log
logger = log.get_logger()

class ComputeEngine(Cluster):
    """
    Google Compute Engine execution mode
    
    Configuration is performed in the compute_engine_config in configs.py
    Machines are spawned dynamically and configured thereafter
    """
    
    def setup(self):
        gcloud.conf = self.config
        self.working_dir = self.config['working_dir']

        execute(gcloud.init)
        execute(gcloud.configure_ssh)

        try:
            execute(gcloud.create_instances)
            execute(gcloud.attach_disk)
            execute(gcloud.mount_disk)
        except gcloud.ExistingInstancesException:
            resume = Prompt("Resume cluster with the following configuration? (y/n) %s" % self.config, "y")
            if not resume.prompt():
                raise Exception("Cluster could not be createcd.")
            resume_mode = MultiPrompt("Reuse cluster instances but fully install/configure (f), configure only (p), or assume complete setup (c),? (f/p/c)")
            resume_mode_answer = resume_mode.prompt()
            if resume_mode_answer == "p":
                return ResumeMode.CONFIGURE
            elif resume_mode_answer == "c":
                return ResumeMode.RESUME

        execute(maintenance.update_package_cache)
        #execute(maintenance.upgrade)
        execute(maintenance.install_dependencies)
        execute(maintenance.set_java_home)
        execute(maintenance.set_key)
        execute(maintenance.set_up_dir, self.working_dir)

        return ResumeMode.FULL_SETUP

    def shutdown(self):
        execute(gcloud.delete_instances)
        
        
class Local(Cluster):
    """
    Local cluster execution, i.e. not dynamically spawned by an IaaS provider
    
    Hosts must be configured in the local_cluster_config in configs.py
    Assumes we do not have root but the standard build tool chain installed (java, git, maven, ...)
    """
    
    def setup(self):
        local.conf = self.config
        self.working_dir = self.config['working_dir']
        execute(local.init)
        execute(maintenance.set_java_home)
        execute(maintenance.set_key)
        execute(maintenance.set_up_dir, self.working_dir)

        resume_mode = MultiPrompt("Reuse cluster instances but fully install/configure (f), configure only (p), or assume complete setup (c),? (f/p/c)")
        resume_mode_answer = resume_mode.prompt()
        if resume_mode_answer == "p":
            return ResumeMode.CONFIGURE
        elif resume_mode_answer == "c":
            return ResumeMode.RESUME

        return ResumeMode.FULL_SETUP

    
    def shutdown(self):
        # TODO do some cleanup here
        pass