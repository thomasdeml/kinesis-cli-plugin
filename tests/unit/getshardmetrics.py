from awscli.testutils import unittest
from kinesis.getshardmetrics import GetShardMetricsCommand


class GetShardMetrics(unittest.TestCase):
 def setUp(self):
        self.rootdir = tempfile.mkdtemp()
