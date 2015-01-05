#
# This file defines the available targets for the fabric command "fab"
#
# For a list of all cluster commands, execute
#
# $ fab -l
#
# Available commands:
#
#     flink.configure
#     flink.copy_log_master
#     flink.copy_log_slaves
#     flink.get_flink_dist_path
#     flink.install
#     flink.master
#     flink.pull
#     flink.pull_from_master
#     flink.run_jar
#     flink.slaves
#     gcloud.authenticate
#     gcloud.configure_ssh
#     gcloud.create_config
#     gcloud.create_instances
#     gcloud.delete_instances
#     gcloud.install_gcloud
#     hadoop.configure
#     hadoop.delete_data_slaves
#     hadoop.install
#     hadoop.master
#     hadoop.pull
#     hadoop.pull_from_master
#     hadoop.run_jar
#     hadoop.set_java_home
#     hadoop.slaves
#     maintenance.generate_key
#     maintenance.install
#     maintenance.install_dependencies
#     maintenance.pull_from_master
#     maintenance.set_java_home
#     maintenance.set_key
#     maintenance.update_package_cache
#     maintenance.upgrade
#     tez.configure
#     tez.get_tez_tarball_path
#     tez.install

from cluster import gcloud, local, maintenance, hadoop, flink, tez, utils

# the default execution mode
# either gcloud for GCE or local for local cluster
default_mode = "gcloud"

if default_mode == "gcloud":
    gcloud.init()
elif default_mode == "local":
    local.init()