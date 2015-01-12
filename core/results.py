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


from os.path import basename

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


def gen_plot(suite_id=None):
    if not plotting_support:
        logger.error("Plotting was attempted but matplotlib could not be loaded previously!")
        return None
    with DB() as db:
        # default figure
        plt.figure(1)
        plt.subplots_adjust(hspace=.5)
        c = db.cursor()
        if suite_id:
            c.execute("select distinct bench_id from results where suite_id = ?", (suite_id,))
            filename = suite_id + ".png"
        else:
            c.execute("select distinct bench_id from results")
            filename = "all_results.png"
        bench_ids = c.fetchall()
        for (i, (bench_id,)) in enumerate(bench_ids):
            print bench_id
            # subfigure
            plt.subplot(len(bench_ids), 1, i+1)
            c.execute("select start_time, duration from results where bench_id=? order by start_time asc", (bench_id,))
            results = c.fetchall()
            timestamps, y = zip(*results)
            labels = [datetime.datetime.fromtimestamp(int(timestamp)).strftime("%d.%m.") for timestamp in timestamps]
            x = range(1, len(results)+1)
            y = map(lambda val: val / 60 if val else 0, y)
            #labels = x
            plt.bar(x, y, align="center", width=0.5)
            plt.xticks(x, labels)
            plt.xlabel('date')
            plt.ylabel('run time (minutes)')
            plt.title(bench_id)
        plt.savefig(filename)
        return filename

