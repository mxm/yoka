import getpass

USER = getpass.getuser()

"""
General config
"""
config = {
    # path where system are set up
    'working_dir' : "/tmp/yoka-dir/",
    # path where additional storage is mounted (e.g. for hdfs)
    'storage_path' : "/home/%s/mnt" % USER,
    'size_mem' : 7500,
    'num_cores' : 2,
}

"""
Standard Local Cluster config

"""
local_cluster_config = {
    # address format: (internal_address, external_address)
    # the internal addresses are used for internal cluster communication
    # the external addresses are necessary when controlling from outside
    # in case, this is started from a machine from inside the cluster, internal and external addresses can be identical

    # the address of the master machine
    'master' : ("locahost", "localhost"),
    # addresses of the slave machines
    'slaves' : [
        # list of machines, e.g.
        ("localhost", "localhost"),
    ],
    # user name for ssh login
    'user' : USER,
    # absolute path to the local ssh key file for authentication
    'ssh_key' : "~/.ssh/id_rsa",
    'working_dir' : config['working_dir'],
    'num_cores' : config['num_cores'],
    'size_mem' : config['size_mem'],
}

"""
Standard Google Compute Engine config

"""
compute_engine_config = {
    'num_workers' : 2,
    'project_name' : "braided-keel-768",
    'zone' : "europe-west1-c",
    'machine_type' : "n1-standard-2",
    'num_cores' : config['num_cores'],
    'size_mem' : config['size_mem'],
    'disk_image' : "debian-7-backports",
    'prefix' : "benchmark-",
    'disk_space_gb' : 20,
    'disk_type' : 'pd-standard', # change to pd-ssd for ssd,
    'disk_mount_path' : config['storage_path'],
    'working_dir' : config['working_dir'],
}


"""
Standard Flink config

"""
flink_config = {
    'git_repository' : "https://github.com/apache/flink",
    'git_commit' : "master",
    'num_task_slots' : 8,
    'parallelization' : 1,
    'taskmanager_heap' : 512,
    'jobmanager_heap' : 256,
    'taskmanager_num_buffers' : 2048,
    'jvm_opts' : "",
    'extra_config_entries' : [
        # additional entries can be added like this:
        # { 'entry' : "taskmanager.memory.size: 1024" },
        # { 'entry' : "another.entry: value" },
    ]
}


"""
Standard Hadoop config

"""
hadoop_config = {
    'source' : "http://mirror.arcor-online.net/www.apache.org/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz",
    'data_path' : config['storage_path'],
    'replication_factor' : 3,
    # memory in mb
    'scheduler_min_mem' : 128,
    'scheduler_max_mem' : int(config['size_mem'] * 0.8),
    'scheduler_min_vcores' : 1,
    'scheduler_max_vcores' : config['num_cores'],
    'nodemanager_max_mem' : int(config['size_mem'] * 0.8),
    'nodemanager_max_vcores' : config['num_cores'],
    # secure mode (kerberos) true or false
    'secure' : True,
    'realm' : "data-artisans.com",
}


"""
Standard Tez config

"""
tez_config = {
        'git_repository' : 'https://github.com/apache/tez.git',
        'git_commit' : 'master',
        'path' : "/home/%s/tez" % USER,
        'path_client' : "/home/%s/tez_client/" % USER,
}


"""
eMail config

"""
email_config = {
    'smtp_server' : "smtp.gmail.com",
    'smtp_port' : 587,
    'smtp_account' : "iamthemailer@gmail.com",
    'smtp_password' : '',
    'addresses' : ["max@data-artisans.com"],
    'subject' : "Performance test results",
    'text' : "Here are the results."
}
