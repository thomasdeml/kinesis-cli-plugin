import datetime
from mock import Mock 

from kinesis_awscli_plugin.lib.streammetricsgetter import StreamMetricsGetter
from kinesis_awscli_plugin.lib.timeutils import TimeUtils
from mock import MagicMock
from argparse import Namespace

class TestStreamMetricsGetter:
  def setUp(self):
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

  def mock_stream_metrics_getter(self):
    return StreamMetricsGetter(
      self.cloudwatch_helper_mock,
      'test',
      TimeUtils.iso8601(datetime.datetime.utcnow()),
      TimeUtils.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=15)),
    )
      
  def test_get(self):
    metrics_getter = self.mock_stream_metrics_getter()
    metrics = metrics_getter.get(['metric1', 'metric2'])  
    assert len(metrics) == 2
    assert metrics[0].datapoint_average == 6.0
