import datetime
from kinesis_awscli_plugin.shardmetricsgetter import ShardMetricsGetter
from kinesis_awscli_plugin.timeutils import TimeUtils
from mock import MagicMock

class TestShardMetricsGetter:
  def setUp(self):
    self.kinesis_helper_mock = MagicMock()
    self.kinesis_helper_mock.stream_shards = MagicMock(return_value = ['Shard1', 'Shard2'])
    self.cloudwatch_helper_mock = MagicMock()
    self.get_metric_datapoints_response = [
        {
            "Timestamp": "2016-10-22T04:57:00Z", 
            "Average": 12, 
            "Unit": "Bytes"
        }, 
        {
            "Timestamp": "2016-10-22T04:56:00Z", 
            "Average": 2, 
            "Unit": "Bytes"
        }, 
        {
            "Timestamp": "2016-10-22T05:05:00Z", 
            "Average": 4, 
            "Unit": "Bytes"
        }, 
    ]
    self.cloudwatch_helper_mock.get_metric_datapoints = MagicMock(return_value = self.get_metric_datapoints_response)

  def mock_shard_metrics_getter(self):
    return ShardMetricsGetter(
      self.cloudwatch_helper_mock,
      self.kinesis_helper_mock,
      'test',
      TimeUtils.iso8601(datetime.datetime.utcnow()),
      TimeUtils.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=15)),
    )
      
  def test_get(self):
    metrics_getter = self.mock_shard_metrics_getter()
    metrics = metrics_getter.get()  
    assert len(metrics) == 2
    assert metrics[0].datapoint_average == 6.0

  def test_get_shard_datapoints(self):
    metrics_getter = self.mock_shard_metrics_getter()
    values = metrics_getter.get_shard_datapoints('abc')
    assert len(values) == 3
