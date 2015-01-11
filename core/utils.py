from time import time
from cluster.utils import master

class GitRepository(object):

    def __init__(self, repo_url, path):
        self.url = repo_url
        self.path = path

    def clone(self):
        master("git clone %s %s || true" % (self.url, self.path))

    def checkout(self, branch):
        master("cd %s && git checkout %s" % (self.path, branch))

    def maven(self, target):
        master("cd %s && mvn %s > /dev/null" % (self.path, target))

    def get_absolute_path(self):
        return "~/" + self.path


class Timer(object):
    """ 
        A decorator for timing a function
        The run time is saved in the timing_dict
    """

    def __init__(self, timing_dict, description):
        self.timing_dict = timing_dict
        self.description = description

    def __call__(self, method):

        def timing(*args, **kwargs):
            run_time = time()
            try:
                return method(*args, **kwargs)
            finally:
                run_time = time() - run_time
                self.timing_dict[method.__name__] = (self.description, run_time)

        return timing
