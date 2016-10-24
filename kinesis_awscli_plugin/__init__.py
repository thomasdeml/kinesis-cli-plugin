from awscli.customizations.commands import BasicCommand
from kinesis_awscli_plugin.getshardmetrics import GetShardMetricsCommand
from kinesis_awscli_plugin.getstreammetrics import GetStreamMetricsCommand
from kinesis_awscli_plugin.pull import PullCommand
from kinesis_awscli_plugin.push import PushCommand

def awscli_initialize(event_emitter):
    event_emitter.register('building-command-table.kinesis', inject_commands)

def inject_commands(command_table, session, **kwargs):
    command_table['get-shard-metrics'] = GetShardMetricsCommand(session)
    command_table['get-stream-metrics'] = GetStreamMetricsCommand(session)
    command_table['pull'] = PullCommand(session)
    command_table['push'] = PushCommand(session)
