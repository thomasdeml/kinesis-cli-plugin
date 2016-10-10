#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

import kinesis
requires = ['awscli>=1.3.22,<1.3.23',
            'six>=1.1.0',
            'python-dateutil>=2.1']

setup(
    name='awscli-kinesis',
    version=kinesis.__version__,
    description='AWSCLI Kinesis plugin',
    long_description=open('README.rst').read(),
    author='Amazon',
    url='http://aws.amazon.com/cli/',
    packages=find_packages('.', exclude=['tests*']),
    package_dir={'kinesis': 'kinesis'},
    package_data={'kinesis': ['examples/*/*.rst',
                             'examples/*/*/*.rst']},
    install_requires=requires,
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ),
)
