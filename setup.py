
import os
import re

from setuptools import find_packages, setup

install_requires = [
    'urllib3>=1.24.2',
    'PyYAML>=3.13',
    'pytz>=2020.1',
    'pyarrow>=1.0.1',
    'click==8.0.3',
    'pendulum>=2.1.2',
    'pyRFC3339>=1.1',
    'basepy>=0.3.5',
    'redis==3.5.3',
    'boto3>=1.19.12',
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
            'generate_test_data_cli = dataspin.script.generate_test_data:run',
        ],
    },
)
