import sqlite3

DB_FILE = 'results.db'

class DB(object):

    def __enter__(self):
        self.conn = sqlite3.connect(DB_FILE)
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class Results(object):

    def __init__(self, suite):
        self.suite = suite
        self.create_db()

    """ Create db if it does not exist """
    def create_db(self):
        with DB() as db:
            c = db.cursor()
            c.execute('''
            CREATE TABLE IF NOT EXISTS "results" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT,
              "suite_id" TEXT NOT NULL,
              "bench_id" TEXT NOT NULL,
              "duration" REAL NOT NULL,
              "start_time" TEXT
            );
            ''')

    def save_results(self):
        with DB() as db:
            c = db.cursor()
            suite_id = self.suite.id
            for benchmark in self.suite.benchmarks:
                data = (suite_id, benchmark.id, benchmark.start_time, benchmark.duration)
                c.execute("""
                INSERT INTO "results"
                  ('suite_id', 'bench_id', 'start_time', 'duration')
                VALUES
                  (?,?,?,?)
                """, data)
