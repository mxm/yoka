import unittest
import os

import core.lib
from core.lib import Cluster, System, Experiment, Benchmark, ClusterSuite
from core.clusters import ComputeEngine
#from systems import Hadoop, Flink
from configs import compute_engine_config, hadoop_config, flink_config

import core.results as results

results.DB_FILE = 'test.db'
core.lib.sleep_time = 0

#hadoop = Hadoop(hadoop_config)
#flink = Flink(flink_config)

class StupidCluster(Cluster):
    
    def setup(self):
        pass

    def shutdown(self):
        pass

class StupidSystem(System):

    def __init__(self, name):
        self.config = {'this': 'is_a_config', 'value': 42}
        self.name = name
    def set_config(self):
        pass
    def install(self):
        pass
    def configure(self):
        pass
    def reset(self):
        pass
    def start(self):
        pass
    def stop(self):
        pass
    def save_log(self, unique_full_path):
        pass
    def __str__(self):
        return self.name


systems = [StupidSystem("stupid1"), StupidSystem("stupid2")]

class TestExperiment(Experiment):

    def setup(self):
        pass

    def run(self):
        print "running experiment"

    def shutdown(self):
        pass

benchmarks = [
    Benchmark("Test1",
              systems=systems,
              experiment = TestExperiment(),
              times = 1
              ),
    Benchmark("Test2",
              systems=systems,
              experiment = TestExperiment(),
              times = 10
              ),
    Benchmark("Test3",
              systems=systems,
              experiment = TestExperiment(),
              times = 3
              ),
]


cluster = StupidCluster({})
name = "TestResults"

class TestResults(unittest.TestCase):

    def setUp(self):
        try:
            os.remove(results.DB_FILE)
        except:
            pass

    def test_save(self):
        suite = ClusterSuite(name, cluster, systems, [], benchmarks)
        # run suite
        suite.execute()
        #check corresponding db entries
        suite_id = suite.id
        suite_uid = suite.uid
        for b in suite.benchmarks:
            data = (suite_uid, suite_id, b.id)
            with results.DB() as db:
                c = db.cursor()
                c.execute("""
                SELECT * FROM results
                WHERE suite_uid = ? and suite_id = ? and
                bench_id = ?
                """, data)
                self.assertEquals(c.fetchall().__len__(), b.times)

    def test_gen_plot(self):
        results.gen_plot(name)

    def test_email_plot(self):
        suite = ClusterSuite(name, cluster, systems, [], benchmarks)
        suite.execute(email_results=True)
        #results.gen_plot(name)

    def tearDown(self):
        pass


unittest.main()
