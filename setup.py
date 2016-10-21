from setuptools import setup

setup(
    name='kinesis_cli_plugin',
    version='0.1',
    py_modules=[
      'kinesis/__init__',
      'kinesis/getshardmetrics',
      'kinesis/shardmetrics', 
      'kinesis/shardmetricsgetter',
      'kinesis/timestringconverter',
      'kinesis/__init__.py'
    ],
    install_requires=[
        'awscli',
    ],
)
