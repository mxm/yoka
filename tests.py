import unittest
import os

from lib import Experiment, ClusterSuite
from clusters import ComputeEngine
from systems import Hadoop, Flink
from configs import compute_engine_config, hadoop_config, flink_config

import results

results.DB_FILE = 'test.db'

cluster = ComputeEngine(compute_engine_config)
hadoop = Hadoop(hadoop_config)
flink = Flink(flink_config)

systems = [hadoop, flink]

class TestExperiment(Experiment):


    start_time = 23
    duration = 42

    def __init__(self, id):
        self.id = id

    def setup(self):
        pass

    def run(self):
        print "running experiment"

    def shutdown(self):
        pass

benchmarks = [
    TestExperiment("Test1"),
    TestExperiment("Test2"),
    TestExperiment("Test3"),
]

class TestResults(unittest.TestCase):

    def setup(self):
        pass

    def test_save(self):
        suite = ClusterSuite("SuiteTest", cluster, systems, benchmarks)
        for b in suite.benchmarks:
            b.run()
        res = results.Results(suite)
        res.save_results()
        suite_id = suite.id
        for b in suite.benchmarks:
            data = (suite_id, b.id, b.start_time, b.duration)
            with results.DB() as db:
                c = db.cursor()
                c.execute("""
                SELECT * FROM results
                WHERE suite_id = ? and bench_id = ? and start_time = ? and duration = ?
                """, data)
                self.assertTrue(c.fetchall().__len__() > 0)

    def tearDown(self):
        os.remove(results.DB_FILE)

unittest.main()
