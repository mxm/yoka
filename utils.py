import config as conf
from fabric.api import local, run

def compile_command(*args):
    return ' '.join(args)

class Command(object):

    def __init__(self, *args):
        self.command = compile_command(*args)

    def get_command(self):
        return self.command

    def execute(self):
        raise NotImplmentedError

class LocalCommand(Command):

    def execute(self, **kwargs):
        return local(self.command, **kwargs)

class RemoteCommand(Command):

    def execute(self):
        run(self.command)
