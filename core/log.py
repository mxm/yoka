import logging
import sys

root = logging.getLogger('yoka')
root.setLevel(logging.DEBUG)

fh = logging.FileHandler('logs/run.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

root.addHandler(ch)
root.addHandler(fh)

def get_logger(*args):
    return root
