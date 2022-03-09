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
@click.pass_context
def start(ctx, project):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.start()

main.add_command(run)
main.add_command(start)