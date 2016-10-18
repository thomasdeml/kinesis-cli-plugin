import tempfile
import datetime
import sys

from mock import Mock 
from mock import MagicMock
from argparse import Namespace
from awscli.customizations.kinesis.getshardmetrics import GetShardMetricsCommand

def test__main():
  print sys.path 
