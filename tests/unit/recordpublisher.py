from kinesis_awscli_plugin.recordpublisher import RecordPublisher 
from mock import MagicMock
from six.moves import queue as Queue
from threading import Event, Lock, Thread
import time
import timeit

class TestRecordPublisher:

  def setUp(self):
    self.kinesis_client_mock = MagicMock()
    self.stop_flag = Event()
    self.queue = Queue.Queue(1000)

  def test_publisher_runs(self):
    t = timeit.timeit(self.run_publisher, number = 1)
    assert t > 3 and t < 7

  def run_publisher(self):
    publisher = RecordPublisher(
      self.stop_flag,
      self.queue,
      self.kinesis_client_mock,
      'TestStream',
      None,
      False,
      100
    ) 
    publisher.start()
    time.sleep(3)
    self.stop_flag.set()

  def test_publish_records_batch_disabled(self):
    record_count = 10
    for i in range(0, record_count):
      self.queue.put({'data': str(i)})
    publisher = RecordPublisher(
      self.stop_flag,
      self.queue,
      self.kinesis_client_mock,
      'TestStream',
      None,
      True,
      100
    ) 
    publisher.start()
    time.sleep(2)
    self.stop_flag.set()
    time.sleep(1)
    print "RECORD COUNT: %s" % record_count
    print "CALL COUNT: %s" % self.kinesis_client_mock.put_record.call_count
    assert self.kinesis_client_mock.put_record.call_count == record_count

  def test_publish_batched(self):
    publisher = RecordPublisher(
      self.stop_flag,
      self.queue,
      self.kinesis_client_mock,
      'TestStream',
      None,
      False, # batching is on
      100
    ) 
    half_of_max_size = publisher.MAX_RECORD_SIZE / 2 
    # put 3 records. 1 record is half the size of max. 
    # Should result in 2 put_record calls
    for i in range(0,3):
      self.queue.put({'data': 'x'*half_of_max_size})
    publisher.start()
    time.sleep(2)
    self.stop_flag.set()
    time.sleep(1)
    print "CALL COUNT: %s" % self.kinesis_client_mock.put_record.call_count
    assert self.kinesis_client_mock.put_record.call_count == 2
