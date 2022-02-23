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
@click.argument("recover", type=click.BOOL)
@click.pass_context
def run(ctx, project, recover):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.run(recover=recover)


main.add_command(run)
