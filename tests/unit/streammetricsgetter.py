import tempfile
import datetime
from mock import Mock 

from kinesis_awscli_plugin.streammetricsgetter import StreamMetricsGetter
from kinesis_awscli_plugin.timeutils import TimeUtils
from mock import MagicMock
from argparse import Namespace

class TestStreamMetricsGetter:
  def setUp(self):
    self.kinesis_mock = MagicMock()

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

  def mock_stream_metrics_getter(self):
    return StreamMetricsGetter(
      self.cloudwatch_mock,
      self.kinesis_mock,
      'test',
      TimeUtils.iso8601(datetime.datetime.utcnow()),
      TimeUtils.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=15)),
    )
      
  def test_get(self):
    metrics_getter = self.mock_stream_metrics_getter()
    metrics = metrics_getter.get(['metric1', 'metric2'])  
    assert len(metrics) == 2
    assert metrics[0].avg() == 6.0

  def test_get_shard_datapoints(self):
    metrics_getter = self.mock_stream_metrics_getter()
    values = metrics_getter.get_metric_datapoints('metric1')
    assert len(values) == 3
