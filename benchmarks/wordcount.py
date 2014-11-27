from fabfile import FlinkBenchmarkSuite, FlinkBenchmark

from cluster.maintenance import install
from cluster.utils import render_template, master
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
        generate_wc_data = render_template(
            "benchmarks/gen_wc_data.sh.mustache",
            self.params
        )
        master("cat <<EOF | bash \n %s \nEOF" % generate_wc_data)

    def run(self):
        run_jar("bla.jar", [1,2,3])

    def shutdown(self):
        pass
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
