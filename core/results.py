import sqlite3

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import Encoders

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
            try:
                # safely get the main method's run time
                # if an exception occurred in the setup, it will not be available
                # if an exception occurred in the main method, it will be available
                _description, start_time, duration = self.benchmark.run_times[self.benchmark.run.__name__]
            except:
                start_time = 0
                duration = 0
            data = (self.suite.uid, self.suite.id,
                    self.benchmark.id,
                    start_time,
                    duration,
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


def gen_plot(suite_id):
    if not plotting_support:
        logger.error("Plotting was attempted but matplotlib could not be loaded previously!")
        return None
    with DB() as db:
        filename = "%s/%s.png" % (RESULTS_DIR, suite_id)
        c = db.cursor()
        # collect all benchmarks of the suite to determine number of plots
        c.execute("select distinct bench_id from results where suite_id = ? order by bench_id ASC", (suite_id,))
        bench_ids = [bid for (bid,) in c.fetchall()]
        num_benchmarks = len(bench_ids)
        # collect all suite executions
        c.execute("select distinct suite_uid from results where suite_id = ?", (suite_id,))
        suite_uids = [uid for (uid,) in c.fetchall()]
        num_suits = len(suite_uids)
        # plot all benchmarks in one plot, share both axes
        figure, axes = plt.subplots(num_benchmarks, sharex=True, sharey=True, figsize=(2*num_suits,2*num_benchmarks))
        for i, bench_id in enumerate(bench_ids):
            axes[i].autoscale_view()
            axes[i].set_title(bench_id)
            axes[i].set_xlabel('date')
            axes[i].set_ylabel('run time (minutes)')
        # plot each benchmark (if it exists) for each suite execution
        x = 0
        for (i, suite_uid) in enumerate(suite_uids):
            # select all run times for each suite execution
            for (bindex, bench_id) in enumerate(bench_ids):
                # select data for this suite id and bench id
                c.execute("select duration, failed from results where bench_id=? and suite_uid=?", (bench_id, suite_uid))
                results = c.fetchall()
                if not results:
                    continue
                maxwidth = 0.8
                width = maxwidth/len(results)
                for (ind, (time, failed)) in enumerate(results):
                    pos = x - (float(ind)/len(results)) + width
                    if failed:
                        axes[bindex].text(pos, 1, "failed", color="red", rotation=90, verticalalignment="bottom")
                    else:
                        time /= 60
                        axes[bindex].bar(pos, time, width=width, alpha=0.8, color="green")
            x += 2
        # construct labels, convert suite_uid to timestamp
        labels = [datetime.datetime.fromtimestamp(int(s.split("_")[-1])).strftime("%d.%m.%Y") for s in suite_uids]
        plt.setp(axes, xticks=range(0, len(labels)*2, 2), xticklabels=labels)
        # rest
        figure.autofmt_xdate()
        figure.tight_layout()
        plt.savefig(filename)
        return filename

