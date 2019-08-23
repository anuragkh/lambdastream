#!/usr/bin/env python
import os

from Cython.Build import cythonize
from setuptools import setup

packages = ['lambdastream', 'lambdastream.aws', 'lambdastream.channels', 'lambdastream.channels.jiffy',
            'lambdastream.channels.jiffy.storage', 'lambdastream.channels.jiffy.directory',
            'lambdastream.channels.jiffy.lease', 'lambdastream.executors']
lambda_install = os.getenv('LAMBDA_INSTALL', 'false')
if lambda_install:
    print('Lambda install detected, skipping packaging some modules')
    packages = ['lambdastream', 'lambdastream.aws', 'lambdastream.channels', 'lambdastream.channels.jiffy',
                'lambdastream.channels.jiffy.storage', 'lambdastream.channels.jiffy.directory',
                'lambdastream.channels.jiffy.lease']

setup(
    name='lambdastream',
    version='0.1.0',
    description='Basic Streaming on Lambda',
    author='Anurag Khandelwal',
    author_email='anuragk@berkeley.edu',
    url='https://www.github.com/anuragkh/lambdastream',
    package_dir={'lambdastream': 'lambdastream'},
    packages=packages,
    ext_modules=cythonize('lambdastream/*.pyx'),
    setup_requires=['pytest-runner>=2.0,<4.0'],
    tests_require=['pytest-cov', 'pytest>2.0,<4.0'],
    install_requires=['redis', 'cloudpickle', 'Cython', 'numpy', 'thrift', 'msgpack']
)
