from utils import run_command
import sys

class TestGetShardMetrics:

  def __init__(self):
    self.stream_name = 'MetricsTest'

  def setUp(self):
    
    pass

  def test_get_shard_metrics(self):
    command = 'aws kinesis get-shard-metrics --stream-name ' + self.stream_name
    has_shard_metrics = False
    for line in run_command(command.split()):
      if '\"ShardMetrics\":' in line:
        has_shard_metrics = True
    assert has_shard_metrics


  def test_get_stream_metrics(self):
    command = 'aws kinesis get-stream-metrics --stream-name ' + self.stream_name
    has_stream_metrics = False
    for line in run_command(command.split()):
      if '\"Metrics\":' in line:
        has_stream_metrics = True
    assert has_stream_metrics




