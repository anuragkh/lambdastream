#!/usr/bin/env python
import os

from Cython.Build import cythonize
from setuptools import setup

jiffy_packages = ['jiffy', 'jiffy.directory', 'jiffy.lease', 'jiffy.storage']
operator_packages = ['operator', 'operator.aws', 'operator.channels']
lambdastream_packages = ['lambdastream', 'lambdastream.executors']
package_dir={'lambdastream': 'lambdastream', 'operator': 'operator', 'jiffy': 'jiffy'}

packages = jiffy_packages + operator_packages + lambdastream_packages
lambda_install = os.getenv('LAMBDA_INSTALL', 'false')
if lambda_install:
    print('Lambda install detected, skipping packaging some modules')
    packages = jiffy_packages + operator_packages
    package_dir = {'operator': 'operator', 'jiffy': 'jiffy'}

setup(
    name='lambdastream',
    version='0.1.0',
    description='Basic Streaming on Lambda',
    author='Anurag Khandelwal',
    author_email='anuragk@berkeley.edu',
    url='https://www.github.com/anuragkh/lambdastream',
    package_dir=package_dir,
    packages=packages,
    ext_modules=cythonize('operator/*.pyx'),
    setup_requires=['pytest-runner>=2.0,<4.0'],
    tests_require=['pytest-cov', 'pytest>2.0,<4.0'],
    install_requires=['redis', 'cloudpickle', 'Cython', 'numpy', 'thrift', 'msgpack']
)
