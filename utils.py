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
        master("cd %s && mvn %s" % (self.path, target))

    def get_absolute_path(self):
        return "~/" + self.path