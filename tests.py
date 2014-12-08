import unittest
import os

from core.lib import System, Experiment, Benchmark, ClusterSuite
from core.clusters import ComputeEngine
#from systems import Hadoop, Flink
from configs import compute_engine_config, hadoop_config, flink_config

import core.results as results

results.DB_FILE = 'test.db'

cluster = ComputeEngine(compute_engine_config)
#hadoop = Hadoop(hadoop_config)
#flink = Flink(flink_config)

class StupidSystem(System):

    def __init__(self, name):
        self.name = name

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


class TestResults(unittest.TestCase):

    def setup(self):
        try:
            os.remove(results.DB_FILE)
        except:
            pass

    def test_save(self):
        suite = ClusterSuite("SuiteTest", cluster, systems, [], benchmarks)
        # skip setup and shutdown, just run the tests
        suite.run()
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

    def tearDown(self):
        pass

#class TestGCloud(unittest.TestCase):


unittest.main()
