#!/usr/bin/env python
from setuptools import setup
from Cython.Build import cythonize

setup(
    name='lambdastream',
    version='0.1.0',
    description='Basic Streaming on Lambda',
    author='Anurag Khandelwal',
    author_email='anuragk@berkeley.edu',
    url='https://www.github.com/anuragkh/lambdastream',
    package_dir={'lambdastream': 'lambdastream'},
    packages=['lambdastream', 'lambdastream.channels', 'lambdastream.executors', 'lambdastream.channels.jiffy',
              'lambdastream.channels.jiffy.storage', 'lambdastream.channels.jiffy.directory',
              'lambdastream.channels.jiffy.lease'],
    ext_modules=cythonize('lambdastream/*.pyx'),
    setup_requires=['pytest-runner>=2.0,<4.0'],
    tests_require=['pytest-cov', 'pytest>2.0,<4.0'],
    install_requires=['redis', 'cloudpickle', 'Cython', 'numpy', 'thrift', 'msgpack', 'psutil']
)
