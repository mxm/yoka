import sqlite3

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import Encoders

from utils import Timer

from configs import email_config as conf

import log

logger = log.get_logger(__name__)

# these are optional
try:
    import matplotlib.pyplot as plt
    import datetime
    plotting_support = True
except:
    plotting_support = False
    logger.warn("Matplotlib could not be loaded, you will not be able to generate plots!")


from os import mkdir
from os.path import basename, exists

RESULTS_DIR = "results"
DB_FILE = "results.db"

# create results dir if it does not exist
if not exists(RESULTS_DIR):
    mkdir(RESULTS_DIR)


class DB(object):

    def __enter__(self):
        self.conn = sqlite3.connect("%s/%s" % (RESULTS_DIR, DB_FILE))

        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()

class Result(object):

    def __init__(self, suite, benchmark=None, log_paths={}):
        self.suite = suite
        # benchmark may be None if just the suite should be created
        self.benchmark = benchmark
        self.log_paths = log_paths
        self.create_db()
        self.convert_to_new_schema()


    def save(self, suite_failed=False, benchmark_failed=False):
        with DB() as db:
            c = db.cursor()

            # create suite entry if it does not exist
            c.execute("""select id from suites where name = ? and start_time = ?""",
                      (self.suite.id, self.suite.start_time))
            result = c.fetchone()
            if result:
                suite_id = result[0]
            else:
                c.execute("""insert into suites (name, start_time, failed)
                                        values  (?, ?, ?)
                        """, (self.suite.id, self.suite.start_time, suite_failed))
                suite_id = c.lastrowid

            # create benchmark entry
            if self.benchmark:
                start_time, duration = Timer.get_run_times(self.benchmark.run_times, self.benchmark.run.__name__)
                data = (suite_id,
                        self.benchmark.id,
                        start_time,
                        duration,
                        benchmark_failed
                        )
                c.execute("""
                INSERT INTO "runs"
                  ('suite_id', 'bench_name', 'start_time', 'duration', 'failed')
                VALUES
                  (?,?,?,?,?)
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

def send_email(filename, additional_text=""):
    FROM = conf['smtp_account']
    TO = conf['addresses']

    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = ', '.join(TO)
    msg['Subject'] = conf['subject']

    TEXT = "%s\n\n%s" % (conf['text'], additional_text)
    msg.attach(MIMEText(TEXT))

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(open(filename, 'rb').read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % basename(filename))
    msg.attach(part)

    server = smtplib.SMTP(conf['smtp_server'], conf['smtp_port'])
    server.ehlo()
    server.starttls()
    server.login(conf['smtp_account'], conf['smtp_password'])
    server.sendmail(FROM, TO, msg.as_string())
    server.close()
    print 'Mail sent successfully.'


def gen_plot(suite_name):
    if not plotting_support:
        logger.error("Plotting was attempted but matplotlib could not be loaded previously!")
        return None
    with DB() as db:
        filename = "%s/%s.png" % (RESULTS_DIR, suite_name)
        c = db.cursor()
        # collect all benchmarks of the suite to determine number of plots
        c.execute("""select distinct bench_name from runs
                     join suites s
                     where s.id = suite_id and s.name = ?
                     order by bench_name ASC""", (suite_name,))
        bench_names = [bname for (bname,) in c.fetchall()]
        num_benchmarks = len(bench_names)
        # collect all suite executions
        c.execute("""select id, name, start_time, failed from suites
                    where name = ?
                    order by start_time asc""", (suite_name,))
        suite_runs = c.fetchall()
        num_suits = len(suite_runs)
        # plot all benchmarks in one plot, share both axes
        step_len = 2
        figure, axes = plt.subplots(num_benchmarks + 1, sharex=True, sharey=False,
                                    figsize=(step_len*num_suits, step_len*num_benchmarks))
        # status
        axes[0].set_title("Setup and Generation", fontsize=20)
        axes[0].axis("off")
        # benchmarks
        for i, bench_name in enumerate(bench_names, start=1):
            axes[i].autoscale_view()
            axes[i].set_title(bench_name)
            axes[i].set_xlabel('date')
            axes[i].set_ylabel('run time (minutes)')
            # labels on both sides and top
            axes[i].tick_params(labeltop=False, labelright=True)
        # plot each benchmark (if it exists) for each suite execution
        x = 0
        labels = []
        bar_space = float(min(1.5, step_len)) # use max 1.5 of space for ALL results
        for suite_id, suite_name, start_time, suite_failed in suite_runs:

            if suite_failed:
                axes[0].text(x + (step_len)/2, 0, "Setup/Generate failed",
                             verticalalignment="bottom", horizontalalignment="center",
                             color="red", fontsize=20)
            else:
                axes[0].text(x + (step_len)/2, 0, "OK",
                             verticalalignment="bottom", horizontalalignment="center",
                             color="green", fontsize=20)

            # select all run times for each suite execution
            for index, bench_name in enumerate(bench_names, start=1):
                # select data for this suite id and bench id
                c.execute("""select duration, failed from runs
                             where bench_name=? and suite_id=?
                            """, (bench_name, suite_id))
                results = c.fetchall()
                if not results:
                    # plot a "no data" label
                    results = [(0, -1)]
                width = bar_space / len(results)
                pos = x + (step_len - bar_space)/2
                # draw result bars
                for ind, (duration, failed) in enumerate(results):
                    if failed == 1:
                        # benchmark failed
                        axes[index].text(pos+width/2, 1, "failed", color="red", rotation=90,
                                         verticalalignment="bottom", horizontalalignment="center")
                    elif failed == -1:
                        # no data
                        axes[index].text(pos+width/2, 1, "no data", color="blue",
                                         verticalalignment="bottom", horizontalalignment="center")
                    else:
                        # plot run time
                        duration /= 60
                        axes[index].bar(pos, duration, width=width, alpha=0.8, color="green", align="edge")
                    pos += width
            x += step_len
            # construct labels
            labels.append(datetime.datetime.fromtimestamp(int(start_time)).strftime("%d.%m.%Y"))
        plt.setp(axes, xticks=range(step_len/2, len(labels)*step_len, step_len), xticklabels=labels)
        # rest
        figure.autofmt_xdate()
        figure.tight_layout()
        plt.savefig(filename)
        return filename



""" Create db if it does not exist """
def create_db():
    with DB() as db:
        c = db.cursor()
        ''' Old Schema
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
        '''
        # New Schema
        c.execute('''
                CREATE TABLE if not exists "suites" (
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "name" TEXT NOT NULL,
                  "start_time" TEXT NOT NULL,
                  "failed" INTEGER NOT NULL
                  );
            ''')
        c.execute('''
                CREATE TABLE if not exists "runs" (
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "suite_id" INTEGER NOT NULL,
                  "bench_name" TEXT NOT NULL,
                  "duration" REAL NOT NULL,
                  "start_time" TEXT NOT NULL,
                  "failed" INTEGER NOT NULL
                  );
        ''')
        c.execute('''
                CREATE TABLE IF NOT EXISTS "logs" (
                  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                  "id_result" TEXT NOT NULL,
                  "system" TEXT NOT NULL,
                  "log_path" TEXT NOT NULL
                );
        ''')

def convert_to_new_schema():
    with DB() as db:
        c = db.cursor()
        # check if suites table exists (if so, we don't have to migrate)
        c.execute("SELECT 1 from sqlite_master WHERE type = 'table' and name = 'results'")
        result = c.fetchone()
        if result:
            logger.info("old table found - starting migration to new schema")
            #1 create suites table
            #2 insert suites
            c.execute("""select suite_uid, bench_id, start_time, duration, failed from results order by suite_uid""")
            suite_uid_old = None
            for suite_uid, bench_name, start_time, duration, failed in c.fetchall():
                suite_name, start_time = suite_uid.split("_")
                if suite_uid != suite_uid_old:
                    # create an entry in the suites table
                    #print suite_name, start_time
                    c.execute("""insert into suites (name, start_time, failed)
                             values (?, ?, 0);
                          """, (suite_name, start_time))
                    suite_id = c.lastrowid
                    suite_uid_old = suite_uid
                #3 create new results table
                c.execute("""insert into runs (suite_id, bench_name, duration, start_time, failed)
                        values (?, ?, ?, ?, ?)
                      """, (suite_id, bench_name, duration, start_time, failed))
            #3 rename old tables
            c.execute("alter table results rename to _results_old_schema")


create_db()
convert_to_new_schema()