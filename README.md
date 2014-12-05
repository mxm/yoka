py-bench
========

py-bench is a framework for running benchmarks on PaaS services. Currently, only Google Compute Engine is supported but py-bench can be extended to support other PaaS providers.

With py-bench, you can define cluster suites which benchmark a set of experiments.
Experiments depend on systems which can be started along with the experiments.
Right now, py-bench supports HDFS and Flink as configurable systems.

Install
-------

Install Python 2.7, then run

    git clone https://github.com/mxm/flink-perf-new
    ./install.sh

The installation script will install the dependencies listed in requirements.txt

Configure
---------

All standard configs are defined in configs.py. The configs can be modified
for a benchmark run by overriding values.


Run
---

To run py-bench, execute

    ./pybench.sh

Look at the provided examples to get an idea how to write your own runs.

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