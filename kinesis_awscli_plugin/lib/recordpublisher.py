import logging
import hashlib
from sys import stdin, stderr, stdout, exc_info
from datetime import datetime
from six.moves import queue as Queue

from awscli.errorhandler import ServerError

from kinesis_awscli_plugin.lib.retry import ExponentialBackoff
from kinesis_awscli_plugin.lib.threads import BaseThread

logger = logging.getLogger(__name__)


class RecordPublisher(BaseThread):

    MAX_RECORD_SIZE = 50 * 1024
    MAX_TIME_BETWEEN_PUTS = 5

    def __init__(self, stop_flag, queue, kinesis_helper, stream_name,
                 partition_key, batch_disabled, push_delay):

        super(RecordPublisher, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_helper = kinesis_helper
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.batch_disabled = batch_disabled
        self.push_delay = push_delay

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        unput_data = ''
        last_record_put_time = datetime.now()
        while True:
            try:
                queue_entry = self.queue.get(False)
                new_data = queue_entry['data']
                # if batching is turned off we immediately put the data
                if self.batch_disabled and len(new_data) > 0:
                    new_data = self._truncate_if_necessary(
                        new_data, self.MAX_RECORD_SIZE)
                    new_data = new_data.rstrip('\n')
                    self.put_kinesis_record_with_progress(
                        self.get_partition_key(new_data), new_data)
                    continue
                logger.debug('New data: ' + new_data + '\n')
                if self._does_new_data_fit(new_data, unput_data,
                                           self.MAX_RECORD_SIZE):
                    logger.debug('adding data to existing batch')
                    unput_data += new_data
                    if self._is_time_to_put(last_record_put_time,
                                            self.MAX_TIME_BETWEEN_PUTS):
                        self.put_kinesis_record_with_progress(
                            self.get_partition_key(unput_data), unput_data)
                        unput_data = ''
                        last_record_put_time = datetime.now()
                else:
                    self.put_kinesis_record_with_progress(
                        self.get_partition_key(unput_data), unput_data)
                    unput_data = self._truncate_if_necessary(
                        new_data, self.MAX_RECORD_SIZE)
                    last_record_put_time = datetime.now()
            except Queue.Empty:
                if self.stop_flag.is_set():
                    logger.debug('Publisher is leaving...')
                    break
                else:
                    # Event.wait expects wait time in seconds. Command-line uses milliseconds.
                    self.stop_flag.wait(float(self.push_delay / 1000.0))
        # still need to put remaining records
        if len(unput_data) > 0:
            self.put_kinesis_record_with_progress(
                self.get_partition_key(unput_data), unput_data)

    def get_partition_key(self, data):
        if self.partition_key is None:
            m = hashlib.md5()
            m.update(data)
            return m.hexdigest()
        else:
            return self.partition_key

    def put_kinesis_record_with_progress(self, partition_key, data):
        self.kinesis_helper.put_record(self.stream_name, partition_key, data)
        stdout.write('.')
        stdout.flush()

    def _does_new_data_fit(self, new_data, data_batch, max_size):
        batch_len = len(data_batch)
        new_data_len = len(new_data)
        if new_data_len + batch_len > max_size:
            return False
        else:
            return True

    def _is_time_to_put(self, start_time, max_time):
        span = datetime.now() - start_time
        if span.seconds > max_time:
            return True
        else:
            return False

    def _truncate_if_necessary(self, data, max_size):
        if len(data) > max_size:
            logger.debug("needed to truncate record")
            return data[1..max_size]
        else:
            return data
