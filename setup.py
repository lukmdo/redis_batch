import sys
from setuptools import setup
from setuptools.command.test import test as Command

import redis_batch


install_requires = ['redis']
if sys.version_info < (3,3):
    raise Exception("Minimal python version is 3.3")
elif sys.version_info < (3,4):
    install_requires += ['asyncio']


class TestCommand(Command):
    description = "run tests"
    user_options = []

    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        import pytest
        errno = pytest.main('')
        sys.exit(errno)

setup(
    name='redis_batch',
    version=redis_batch.__version__,
    description='redis-py extension to async-batch commands',
    long_description=open("README.rst").read(),
    url='http://github.com/lukmdo/redis_batch',
    maintainer='lukmdo',
    maintainer_email='me@lukmdo.com',
    keywords=['redis', 'async-redis', 'redis-pipe', 'redis-pipeline'],
    license='MIT',
    packages=['redis_batch'],
    install_requires=install_requires,
    tests_require=['pytest'],
    cmdclass={'test': TestCommand},
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.3',
    ]
)
