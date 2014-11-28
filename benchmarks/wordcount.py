from fabfile import FlinkBenchmarkSuite, FlinkBenchmark

from cluster.maintenance import install
from cluster.utils import master, render_template, exec_bash
from cluster.flink import run_jar

#ARGS="$HDFS_WC $HDFS_WC_OUT"
#echo "running wc with args $ARGS"
#$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-WordCount.jar $ARGS


class Run(FlinkBenchmark):

    def __init__(self, parameters):
        self.params = parameters

    def setup(self):
        super(Run, self).setup()
        master(lambda: install("wget"))
        master(lambda: install("ruby"))
        master(lambda: install("bzip2"))
        master(lambda: install("aspell"))
        generate_wc_data = render_template(
            "benchmarks/gen_wc_data.sh.mustache",
            self.params
        )
        master(lambda: exec_bash(generate_wc_data))

    def run(self):
        fun = lambda: run_jar("benchmarks",
                              "flink-java-examples-0.8-incubating-SNAPSHOT-WordCount.jar",
                              ["file:///tmp/wc-data/generated-wc.txt", "/tmp/wc-out/"])
        master(fun)

    def shutdown(self):
        super(Run, self).shutdown()



experiments = [

    Run({
        'num_lines' : 1000,
    }),

    Run({
        'num_lines' : 1000000,
    }),

]

SUITE = FlinkBenchmarkSuite(experiments)
