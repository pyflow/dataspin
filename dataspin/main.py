from .project import ProjectConfig
import click
from basepy.log import logger
from .core import SpinEngine

logger.add('stdout')

@click.group()
def main():
    pass

@click.command()
@click.argument("project", type=click.Path(exists=True))
def run(project):
    conf = ProjectConfig.load(project)
    engine = SpinEngine(conf)
    engine.run()

main.add_command(run)