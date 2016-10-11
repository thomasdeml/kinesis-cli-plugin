import tempfile
import datetime
from mock import Mock 

from awscli.testutils import unittest
from kinesis.getshardmetrics import GetShardMetricsCommand


class TestGetShardMetrics(unittest.TestCase):
 def setUp(self):
   self.rootdir = tempfile.mkdtemp()
   self.mock_args = Mock(
     stream_name = 'test',
     metric_name = 'IncomingRecords',
     statistic = 'Sum',
     start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=15),
     end_time = datetime.datetime.utcnow(),
  )

 def test_collect_args(self):
   collect_args(self.mock_args)
      
 
