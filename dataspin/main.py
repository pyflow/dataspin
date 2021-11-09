from .project import ProjectConfig
import click
from basepy.log import logger

logger.add('stdout')

@click.group()
def main():
    pass

@click.command()
@click.argument("project", type=click.Path(exists=True))
def run(project):
    conf = ProjectConfig.load(project)

main.add_command(run)