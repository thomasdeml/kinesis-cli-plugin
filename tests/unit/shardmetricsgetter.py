import tempfile
import datetime
from mock import Mock 

from kinesis_awscli_plugin.shardmetricsgetter import ShardMetricsGetter
from kinesis_awscli_plugin.timestringconverter import TimeStringConverter
from mock import MagicMock
from argparse import Namespace

class TestShardMetricsGetter:
  def setUp(self):
    self.kinesis_mock = MagicMock()
    self.describe_stream_response = {
      'StreamDescription': {
        'Shards': [
          {'ShardId': '1'}, 
          {'ShardId': '2'}
         ]
       }
    }
    self.kinesis_mock.describe_stream = MagicMock(return_value = self.describe_stream_response)

    self.cloudwatch_mock = MagicMock()
    self.get_metric_statistics_response = {
      "Datapoints": [
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
    }
    self.cloudwatch_mock.get_metric_statistics = MagicMock(return_value = self.get_metric_statistics_response)

  def mock_shard_metrics_getter(self):
    return ShardMetricsGetter(
      self.cloudwatch_mock,
      self.kinesis_mock,
      'test',
      TimeStringConverter.iso8601(datetime.datetime.utcnow()),
      TimeStringConverter.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=15)),
    )
      
  def test_get(self):
    metrics_getter = self.mock_shard_metrics_getter()
    metrics = metrics_getter.get()  
    assert len(metrics) == 2
    assert metrics[0].avg() == 6.0

  def test_get_shard_datapoints(self):
    metrics_getter = self.mock_shard_metrics_getter()
    values = metrics_getter.get_shard_datapoints('abc')
    assert len(values) == 3
 
  def test_metric_values(self):
    metrics_getter = self.mock_shard_metrics_getter()
    values = metrics_getter.metric_values(self.get_metric_statistics_response['Datapoints'], 'Average')
    assert len(values) == 3
