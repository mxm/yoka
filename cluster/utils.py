from __future__ import with_statement
import config as conf
from fabric.api import local, run, put

import pystache

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


def process_template(module, template, context, destination):
    renderer = pystache.Renderer()
    template_path = "%s/%s/%s" % (conf.TEMPLATE_PATH, module, template)
    config_content = renderer.render_path(
        template_path,
        context
    )
    src = "%s/%s" % (conf.TMP, template[:-9])
    with open(src, 'w') as f:
        f.write(config_content)
    put(src, "%s" % destination)
