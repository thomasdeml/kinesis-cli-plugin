import time
from kinesis_awscli_plugin.lib.recordspuller import RecordsPuller
from mock import MagicMock
from six.moves import queue as Queue
from threading import Event

class TestRecordsPuller:
  def setUp(self):
    self.kinesis_mock = MagicMock()
    self.kinesis_mock.get_records = MagicMock(return_value = {'Records': [1,2,3], 'NextShardIterator': 'new_iterator'})
    self.stop_flag = Event()
    self.queue = Queue.Queue()
    self.kinesis = MagicMock()
    self.rp = RecordsPuller(
      self.stop_flag,
      self.queue,
      self.kinesis_mock,
      'test',
      100,
      3)


  def test_pull(self):
    self.rp.start()
    time.sleep(4)      
    print "qsize: %s" % self.queue.qsize()
    # loop should at least run twice to make sure it works
    assert self.queue.qsize() > 2
