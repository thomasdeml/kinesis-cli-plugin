from awscli.customizations.commands import BasicCommand
from kinesis.getshardmetrics import GetShardMetricsCommand

def awscli_initialize(event_emitter):
    event_emitter.register('building-command-table.kinesis', inject_commands)

def inject_commands(command_table, session, **kwargs):
    command_table['get-shard-metrics'] = GetShardMetricsCommand(session)
