from setuptools import setup

setup(
    name = 'kinesis_awscli_plugin',
    version = '0.1',
    packages = ['kinesis_awscli_plugin'],
    package_data={'kinesis_awscli_plugin': ['*.rst', 'kinesis_awscli_plugin/examples/kinesis/*.rst']},
    include_package_data = True,
#    install_requires = [
#        'awscli>1.10',
#    ],
    author = 'Thomas Deml',
    author_email = 'thomas.deml@gmail.com',
    description = 'An AWS Command-line Interface plugin for AWS Kinesis',
    keywords = 'AWS Kinesis CLI',
)
