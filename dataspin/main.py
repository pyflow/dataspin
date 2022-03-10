from dataspin.project import ProjectConfig
import click
from basepy.log import logger
from dataspin.core import SpinEngine

logger.add('stdout')
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.pass_context
def run(ctx, project):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.run()

@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.argument("process_name", type=str)
@click.pass_context
def run_process(ctx, project, process_name):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.run_process(process_name)


@click.command()
@click.argument("project", type=click.Path(exists=True))
@click.pass_context
def start(ctx, project):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.start()

main.add_command(run)
main.add_command(start)
main.add_command(run_process)