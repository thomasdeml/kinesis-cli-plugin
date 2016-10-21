from setuptools import setup

setup(
    name='kinesis_cli_plugin',
    version='0.1',
    py_modules=[
      'kinesisregister',
      'kinesis/getshardmetrics',
      'kinesis/shardmetrics', 
      'kinesis/shardmetricsgetter',
      'kinesis/timestringconverter',
    ],
    install_requires=[
        'awscli',
    ],
)
