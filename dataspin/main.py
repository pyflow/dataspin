from dataspin.project import ProjectConfig
import click
from basepy.log import logger
from dataspin.core import SpinEngine

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


if __name__ == '__main__':
    conf = ProjectConfig.load('/Users/yuze/Desktop/dataspin_test.json')
    engine = SpinEngine(conf)
    engine.run()