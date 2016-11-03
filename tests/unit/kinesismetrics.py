import datetime
from mock import Mock 

from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics
from mock import MagicMock

class TestShardMetricsGetter:
  def setUp(self):
    self.datapoints = [
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

  def mock_kinesis_metrics(self):
    return KinesisMetrics(
      'TestMetric',
      self.datapoints,
      'Average'
    )
      
  def test_kinesis_metrics(self):
    metrics = self.mock_kinesis_metrics()
    assert(metrics.metric_id == 'TestMetric')
    assert(metrics.statistic == 'Average')
    assert(metrics.datapoint_average == 6)
    assert(metrics.datapoint_min == 2)
    assert(metrics.datapoint_max == 12)
