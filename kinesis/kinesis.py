from awscli.customizations.kinesis.kinesis_pull import initialize as kinesis_pull_init
from awscli.customizations.kinesis.kinesis_push import initialize as kinesis_push_init
  
def initialize(cli):
  kinesis_pull_init(cli)
  kinesis_push_init(cli)
