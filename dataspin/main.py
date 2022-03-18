import click
from basepy.log import logger
from dataspin.core import SpinManager
import os

logger.add('stdout')
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.pass_context
def run(ctx, project):
    sm = SpinManager()
    sm.load_one(project)
    sm.run()

@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.argument("process_name", type=str)
@click.pass_context
def run_process(ctx, project, process_name):
    sm = SpinManager()
    sm.load_one(project)
    sm.run_process(project, process_name)


@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.pass_context
def start(ctx, project):
    sm = SpinManager()
    if os.path.isfile(project):
        sm.load_one(project)
    elif os.path.isdir(project):
        sm.load_all(project)
    else:
        raise Exception(f'project {project} is not file or directory.')
    sm.start()

main.add_command(run)
main.add_command(start)
main.add_command(run_process)