
import os
import re

from setuptools import find_packages, setup

install_requires = [
    'pytz>=2020.1',
    'pyarrow>=1.0.1',
    'click',
    'pyRFC3339>=1.1',
    'basepy>=0.4a1',
    'boto3>=1.19.12',
    'pendulum>=2.1.2'
    'dataclass_factory',
    'jinja2',
    'boltons',
    'parsy',
    'pulsar-client==2.7.1',
    'cos-python-sdk-v5==1.9.15'
]
# pip install -e '.[test]'
test_requires = [
    'pytest>=6.2.4',
    # 'pytest-asyncio',
    # 'pytest-cov',
    # 'pytest-mock',
]

here = os.path.dirname(os.path.abspath(__file__))
with open(
        os.path.join(here, 'dataspin/__init__.py'), 'r', encoding='utf8'
) as f:
    version = re.search(r'__version__ = \"(.*?)\"', f.read()).group(1)

setup(
    name='dataspin',
    version=version,
    license='MIT',
    packages=find_packages(exclude=['tests.*', 'tests']),
    zip_safe=False,
    install_requires=install_requires,
    platforms='any',
    extras_require={
        'test': test_requires,
    },
    entry_points={
        'console_scripts': [
            'dataspin = dataspin.main:main',
            'dataspin_testgen = dataspin.cli.test_data_generator:run',
        ],
    },
)
