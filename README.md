py-bench
========

py-bench is a framework for running performance tests on PaaS
services. Currently, only Google Compute Engine is supported but py-bench can be
extended to support other PaaS providers.

With py-bench, you can define cluster suites which performance test a set of
experiments. Experiments depend on systems which can be started along with the
experiments. Right now, py-bench supports HDFS and Flink as configurable
systems.

Install
-------

### py-bench

Install Python 2.7, Virtualenv for Python, and the Python development files. In Debian, this command does the trick:

    sudo apt-get install python2.7 python-dev python-virtualenv

Then run:

    git clone https://github.com/mxm/flink-perf-new
    cd flink-perf-new
    ./install.sh

The installation script will install the dependencies listed in requirements.txt.

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

To run py-bench, execute

    ./pybench.sh <runfile>

The runs.py is an example of a runfile. You can use it as base to write your own
runs.


Evaluation
----------

The results of each performance test are stored in the results.db database.  The
"results" table holds the run times. The "logs" table holds the path to the
system logs which are available in the /logs directory.

Development
-----------

To trigger certain actions on the cluster, you can use the fab command.

    source env/bin/activate
    fab -l
    # start flink
    fab flink.master
    fab flink.slaves
    # stop flink
    fab flink.slaves:stop
    fab flink.master:stop
