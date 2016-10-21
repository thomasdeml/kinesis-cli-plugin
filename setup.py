from setuptools import setup

setup(
    name='kinesis_cli_plugin',
    version='0.1',
    packages = ['kinesis'],
    install_requires=[
        'awscli',
    ],
)
