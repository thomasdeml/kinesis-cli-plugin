import tempfile
import datetime
from mock import Mock 

#from awscli.testutils import unittest
from kinesis.getshardmetrics import GetShardMetricsCommand
from mock import MagicMock
from argparse import Namespace

class TestGetShardMetrics(unittest.TestCase):
  def setUp(self):
    self.mock_args = Mock(
      stream_name = 'test',
      metric_name = 'IncomingRecords',
      statistic = 'Sum',
      start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=15),
      end_time = datetime.datetime.utcnow(),
    )
    self.session = MagicMock()
    self.args = Namespace()
    self.globals = Namespace()
    self.globals.region = self.region
    self.globals.endpoint_url = self.endpoint_url
    self.globals.verify_ssl = False    

  def test_run_main(self):
    self.command = GetShardMetricsCommand(self.session)
    self.command._run_main(self.args, self.globals)
      
