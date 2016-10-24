from setuptools import setup

setup(
    name = 'kinesis_awscli_plugin',
    version = '0.1',
    packages = ['kinesis_awscli_plugin'],
    install_requires = [
        'awscli',
    ],
    author = 'Thomas Deml',
    author_email = 'thomas.deml@gmail.com',
    description = 'An AWS Command-line Interface plugin for AWS Kinesis',
    keywords = 'AWS Kinesis CLI',
)
