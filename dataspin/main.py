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
@click.argument("recover", type=click.BOOL)
@click.pass_context
def run(ctx, project, recover):
    sm = SpinManager()
    sm.load_one(project)
    sm.run(recover)

@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.argument("process_name", type=str)
@click.argument("recover", type=click.BOOL)
@click.pass_context
def run_process(ctx, project, process_name, recover):
    sm = SpinManager()
    sm.load_one(project)
    sm.run_process(project, process_name, recover)


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


if __name__ == '__main__':

    sm = SpinManager()
    sm.load_one('/Users/yuze/Desktop/github_code/dataspin/examples/simple.json')
    sm.run_process('/Users/yuze/Desktop/github_code/dataspin/examples/simple.json', 'split and build index', False)