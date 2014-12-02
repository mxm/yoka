from lib import Experiment

from cluster.maintenance import install
from cluster.utils import master, render_template, exec_bash
from cluster.flink import run_jar
from cluster.hadoop import copy_to_hdfs

from fabric.api import env

class WordCount(Experiment):

    def __init__(self, params):
        self.params = params

    def setup(self):
        # generate wc data
        master(lambda: install("wget"))
        master(lambda: install("ruby"))
        master(lambda: install("bzip2"))
        master(lambda: install("aspell"))
        generate_wc_data = render_template(
            "experiments/wordcount_files/gen_wc_data.sh.mustache",
            self.params
        )
        master(lambda: exec_bash(generate_wc_data))
        master(lambda: copy_to_hdfs("/tmp/wc-data/generated-wc.txt",
                                    "generated-wc%d.txt" % self.params['id']))


    def run(self):
        master (lambda: run_jar("experiments/wordcount_files/",
                              "flink-java-examples-0.8-incubating-SNAPSHOT-WordCount.jar",
                              ["hdfs://%s:50040/generated-wc%d.txt"
                               % (env.master, self.params['id']),
                               "hdfs://%s:50040/tmp/wc-out%d/"
                               % (env.master, self.params['id'])
                              ]
                        )
        )

    def shutdown(self):
        master("rm -rf /tmp/wc-data/generated-wc.txt")
