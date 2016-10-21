from setuptools import setup

setup(
    name='kinesis_cli_plugin',
    version='0.1',
    package_dir = {'': 'kinesis_cli_plugin'}
    packages = ['kinesis'],
    install_requires=[
        'awscli',
    ],
)
