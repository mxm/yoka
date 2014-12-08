import sqlite3

DB_FILE = 'results.db'

class DB(object):

    def __enter__(self):
        self.conn = sqlite3.connect(DB_FILE)
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class Result(object):

    def __init__(self, suite, benchmark, log_paths):
        self.suite = suite
        self.benchmark = benchmark
        self.log_paths = log_paths
        self.create_db()

    """ Create db if it does not exist """
    def create_db(self):
        with DB() as db:
            c = db.cursor()
            c.executescript('''
            CREATE TABLE IF NOT EXISTS "results" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT,
              "suite_uid" TEXT NOT NULL,
              "suite_id" TEXT NOT NULL,
              "bench_id" TEXT NOT NULL,
              "duration" REAL,
              "start_time" TEXT,
              "failed" INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "logs" (
              "id" INTEGER PRIMARY KEY AUTOINCREMENT,
              "id_result" TEXT NOT NULL,
              "system" TEXT NOT NULL,
              "log_path" TEXT NOT NULL
            );
            ''')

    def save(self, failed=False):
        with DB() as db:
            c = db.cursor()
            data = (self.suite.uid, self.suite.id,
                    self.benchmark.id,
                    self.benchmark.start_time,
                    self.benchmark.duration,
                    failed
            )
            c.execute("""
            INSERT INTO "results"
              ('suite_uid', 'suite_id', 'bench_id', 'start_time', 'duration', 'failed')
            VALUES
              (?,?,?,?,?,?)
            """, data)
            result_id = c.lastrowid
            for system in self.log_paths.keys():
                path = self.log_paths[system]
                log_data = (result_id, str(system), path)
                c.execute("""
                INSERT INTO "logs"
                  ('id_result', 'system', 'log_path')
                VALUES
                  (?,?,?)
                """, log_data)
