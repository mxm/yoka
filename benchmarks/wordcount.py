from fabfile import FlinkBenchmarkSuite, FlinkBenchmark

from cluster.maintenance import install
from cluster.utils import master, render_template, exec_bash
from cluster.flink import run_jar
from cluster.hadoop import copy_to_hdfs

from fabric.api import env
#ARGS="$HDFS_WC $HDFS_WC_OUT"
#echo "running wc with args $ARGS"
#$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-WordCount.jar $ARGS


class Run(FlinkBenchmark):

    def __init__(self, parameters):
        self.params = parameters

    def setup(self):
        super(Run, self).setup()
        # generate wc data
        master(lambda: install("wget"))
        master(lambda: install("ruby"))
        master(lambda: install("bzip2"))
        master(lambda: install("aspell"))
        generate_wc_data = render_template(
            "benchmarks/gen_wc_data.sh.mustache",
            self.params
        )
        master(lambda: exec_bash(generate_wc_data))
        master(lambda: copy_to_hdfs("/tmp/wc-data/generated-wc.txt",
                                    "generated-wc%d.txt" % self.params['id']))



    def run(self):
        fun = lambda: run_jar("benchmarks",
                              "flink-java-examples-0.8-incubating-SNAPSHOT-WordCount.jar",
                              ["hdfs://%s:50040/generated-wc%d.txt"
                               % (env.master, self.params['id']),
                               "hdfs://%s:50040/tmp/wc-out%d/"
                               % (env.master, self.params['id'])
                              ]
                      )
        master(fun)

    def shutdown(self):
        super(Run, self).shutdown()



experiments = [

    Run({
        'id' : 1,
        'num_lines' : 1000,
    }),

    Run({
        'id' : 2,
        'num_lines' : 1000000,
    }),

]

SUITE = FlinkBenchmarkSuite(experiments)
