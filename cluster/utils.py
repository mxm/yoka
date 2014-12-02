from __future__ import with_statement
import types
from fabric.api import local, run, put, execute
import pystache

def compile_command(*args):
    return ' '.join(args)


class Command(object):

    def __init__(self, *args):
        self.command = compile_command(*args)

    def get_command(self):
        return self.command

    def execute(self):
        raise NotImplementedError()

class LocalCommand(Command):

    def execute(self, **kwargs):
        return local(self.command, **kwargs)

class RemoteCommand(Command):

    def execute(self):
        run(self.command)

def exec_on(command, master_or_slaves):
    if isinstance(command, types.FunctionType):
        execute(command, role=master_or_slaves)
    else:
        execute(lambda: run(command), role=master_or_slaves)

""" Execute command on master """
def master(command):
    exec_on(command, 'master')

""" Execute command on slaves """
def slaves(command):
    exec_on(command, 'slaves')

def exec_bash(script):
    return run("cd /tmp && cat <<'EOF' | bash \n%s \nEOF" % script)


def render_template(template_path, context):
    renderer = pystache.Renderer()
    return renderer.render_path(template_path, context)

def process_template(module, template, context, destination):
    template_path = "%s/%s/%s" % (conf.CLUSTER_TEMPLATE_PATH, module, template)
    config_content = render_template(template_path, context)
    src = "%s/%s" % (conf.TMP, template[:-9])
    with open(src, 'w') as f:
        f.write(config_content)
    put(src, "%s" % destination)
