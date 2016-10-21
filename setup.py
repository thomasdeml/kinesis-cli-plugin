from setuptools import setup

setup(
    name='kinesis_cli_plugin',
    version='0.1',
    py_modules=[
      'kinesisregister.py',
      'kinesis/getshardmetrics.py',
      'kinesis/shardmetrics.py', 
      'kinesis/shardmetricsgetter.py',
      'kinesis/timestringconverter.py',
    ],
    install_requires=[
        'awscli',
    ],
)
