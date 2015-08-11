from time import time
from sys import stdout
import types, datetime
from functools import wraps

from cluster.utils import master

class GitRepository(object):

    def __init__(self, repo_url, path):
        self.url = repo_url
        self.path = path
        self.commit = None
        self.cloned = False
        self.built_using = None

    def clone(self):
        if not self.cloned:
            master("rm -rf %s && git clone %s %s" % (self.path, self.url, self.path))
            self.cloned = True

    def checkout(self, commit):
        if commit != self.commit:
            master("cd %s && git checkout %s" % (self.path, commit))
            self.commit = commit
            self.built_using = None

    def maven(self, target):
        # avoid building multiple times
        if target != self.built_using:
            master("cd %s && mvn %s > /dev/null" % (self.path, target))
            self.built_using = target

    def get_absolute_path(self):
        return "~/" + self.path


class Timer(object):
    """ 
        A decorator for timing a function
        The run time is saved in a dict with the timed methods as key
        The stored item is a tuple (description, start_time, run_time)
    """

    def __init__(self, timings, description):
        if not isinstance(timings, types.DictType):
            raise Exception("Only dicts are supported!")
        self.timings = timings
        self.description = description

    def __call__(self, method):
        
        # this decorator preserves the original function attributes (e.g. name)
        @wraps(method)
        def timing(*args, **kwargs):
            start_time = time()
            try:
                return method(*args, **kwargs)
            finally:
                run_time = time() - start_time
                value = (self.description, int(start_time), run_time)
                self.timings[method.__name__] = value

        return timing
    
    @staticmethod
    def format_run_times(run_times):
        s = ""
        for fun_name, (description, start_time, run_time) in run_times.iteritems():
            timestamp = int(start_time)
            time = datetime.datetime.fromtimestamp(start_time).strftime("%d.%m.%Y %H:%M:%S")
            seconds = int(run_time)
            s += "%s\n%s\n%s hours, %s minutes, %s seconds\n\n" \
                 % (description, time, seconds // 3600, seconds // 60 % 60, seconds % 60)
        return s


class Prompt(object):

    """
        A simple class to ask the user a question and return True if the expected answer was given.
    """
    def __init__(self, message, expected=""):
        self.message = message
        self.expected = expected

    def prompt(self):
        while True:
            print self.message, "\n", "(enter '" + self.expected + "' to confirm.)"
            stdout.flush()
            try:
                input = raw_input()
                if input:
                    return input.lower() == self.expected.lower()
            except EOFError:
                pass

class MultiPrompt(object):

    """
        A simple class to ask the user a question and return the answer.
    """
    def __init__(self, message):
        self.message = message

    def prompt(self):
        while True:
            print self.message, "\n"
            stdout.flush()
            try:
                input = raw_input()
                if input:
                    return input.lower()
            except EOFError:
                pass