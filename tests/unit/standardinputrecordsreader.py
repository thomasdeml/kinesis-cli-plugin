from kinesis_awscli_plugin.standardinputrecordsreader import StandardInputRecordsReader
from mock import MagicMock, patch
from nose.tools import assert_raises
from six.moves import queue as Queue
from threading import Event
import time
from sys import stdin

def fake_stdin_read(self):
  time.sleep(1)
  return 'test'

def fake_stdin_large_line(self):
  time.sleep(1)
  return 'x' * 30000

class TestStandardInputRecordsReader:

  def setUp(self):
    self.mock = MagicMock()
    self.stop_flag = Event()
    self.queue = Queue.Queue(1000)

  @patch.object(StandardInputRecordsReader, 'read_stdin_line', fake_stdin_read) 
  def test_run(self):
    reader = StandardInputRecordsReader(
      self.stop_flag,
      self.queue,
      False
    )
    reader.start()
    time.sleep(2)
    self.stop_flag.set()
    time.sleep(1)
    print "Queue size: %s" % self.queue.qsize()
    # sleeping for 2 seconds should execute fake_stdin_read twice
    assert self.queue.qsize() >= 2

  @patch.object(StandardInputRecordsReader, 'read_stdin_line', fake_stdin_large_line) 
  def test_large_line(self):
    reader = StandardInputRecordsReader(
      self.stop_flag,
      self.queue,
      False
    )

    reader.start()
    time.sleep(2)
    self.stop_flag.set()
    time.sleep(1)
    #BUGBUG: BaseThread swallows exceptions!!! Large lines are not processed.
    assert self.queue.qsize() == 0 

