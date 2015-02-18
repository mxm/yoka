Yoka
====

Yoka is a framework for running performance tests on clusters. Existing clusters
as well as dynamic clusters created by PaaS services are supported. Currently,
only Google Compute Engine is supported as a PaaS service but Yoka can be
extended to support other PaaS providers.

With Yoka, you can define cluster suites which performance test a set of
experiments. Experiments depend on systems which can be started along with the
experiments. Right now, Yoka supports HDFS and Flink as configurable systems.

Install
-------

### Yoka

Install Python 2.7, Virtualenv for Python, and the Python development files. In Debian, this command does the trick:

    sudo apt-get install python2.7 python-dev python-virtualenv

Then run:

    git clone https://github.com/mxm/yoka
    cd yoka
    ./install

The installation script will install the dependencies listed in requirements.txt.

#### Plotting

Optionally, if you want to generate plots, install:

    sudo apt-get install libfreetype6-dev libpng12-dev

Then execute:

    ./install with_plotting

### Google Compute Engine

For using Google Compute Engine, please install the gcloud tool, i.e. using

    curl https://sdk.cloud.google.com | bash

Then authenticate against the Google Compute Engine

    gcloud config set account <ACCOUNTNAME>
    gcloud auth login


Configure
---------

All standard configs are defined in configs.py. By overriding values, the
configs can be modified for a performance test.

Run
---

To run Yoka, execute

    ./yoka run <run_name>

Run files are located in the runs directory. You can use any of the
supplied run files as a base to write your own.

For example, to run runs/example.py execute the following:

    ./yoka run example


Evaluation
----------

The results of each performance test are stored in the results.db database.  The
"results" table holds the run times. The "logs" table holds the path to the
system logs which are available in the /logs directory.

Development
-----------

To trigger certain actions on the cluster, you can use Fabric's fab command.

    # activate virtual environment
    source env/bin/activate
    # list all available actions
    fab -l
    # execute some actions, e.g.
    # start flink
    fab flink.master
    fab flink.slaves
    # stop flink
    fab flink.slaves:stop
    fab flink.master:stop
