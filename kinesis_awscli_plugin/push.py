from datetime import datetime
from dateutil.tz import tzlocal
import hashlib
import logging
import json
import os
from operator import itemgetter
import shelve
from sys import stdin, stderr, stdout, exc_info
from threading import Event, Lock, Thread
import time
import six
from six.moves import queue as Queue
from botocore.vendored import requests
from awscli.errorhandler import ServerError
from awscli.customizations.commands import BasicCommand
from kinesis_awscli_plugin.threads import BaseThread, ExitChecker
from kinesis_awscli_plugin.retry import ExponentialBackoff
from kinesis_awscli_plugin.utils \
     import log_to_stdout,log_to_stderr, endpoint_config, register_ctrl_c_handler
import botocore
import botocore.exceptions
from six.moves import configparser
from sys import stdout
from awscli.errorhandler import ServerError

logger = logging.getLogger(__name__)

class PushCommand(BasicCommand):
    NAME = 'push'
    DESCRIPTION = ('This command pushes records to a Kinesis stream.  '
                   'Standard input is read line by line and sent as record. '
                   'A partition key has to be specified as well.')
    SYNOPSIS = ''
    DEFAULT_PUSH_DELAY = 1000

    ARG_TABLE = [

        {'name': 'stream-name',
         'required': True,
         'help_text': 'Specifies the stream name'},
        
        {'name': 'partition-key',
         'required': False,
         'help_text': 'Specifies the partition key, e.g. $HOSTNAME. If not provided the partition key is random number.'},

        {'name': 'push-delay', 
         'cli_type_name': 'integer',
         'default': DEFAULT_PUSH_DELAY,
         'help_text': 'Specifies the delay in milliseconds between publishing '
                      'two batches of streams. Defaults to 1000 ms.'},

        {'name': 'dry-run', 
         'action': 'store_true',
         'help_text': 'Prints stream data instead of sending to service.'},
    ]

    UPDATE = False
    QUEUE_SIZE = 10000



    def _run_main(self, args, parsed_globals):
        logger.debug("no-batch set to " + str(args.no_batch))
        self.kinesis = self._session.create_client(
            'kinesis', 
            region_name=parsed_globals.region,
            endpoint_url = parsed_globals.endpoint_url,
            verify=parsed_globals.verify_ssl
        )
        register_ctrl_c_handler()
        self._call_push_stdin(args, parsed_globals)
        return 0

    def _call_push_stdin(self, options, parsed_globals):
        threads = []
        queue = Queue.Queue(self.QUEUE_SIZE)
        stop_flag = Event()
        reader = StandardInputRecordsReader(stop_flag, queue)
        reader.start()
        threads.append(reader)
        publisher = RecordPublisher(
            stop_flag, 
            queue, 
            self.kinesis, 
            options.stream_name, 
            options.partition_key,
            true, # enables batching
            int(options.push_delay))
        publisher.start()
        threads.append(publisher)
        self._wait_on_exit(stop_flag)
        reader.join()
        publisher.join()

    def _wait_on_exit(self, stop_flag):
        exit_checker = ExitChecker(stop_flag)
        exit_checker.start()
        try:
            while exit_checker.is_alive() and not stop_flag.is_set():
                exit_checker.join(5)
        except KeyboardInterrupt:
            pass
        logger.debug('Shutting down...')
        stop_flag.set()
        exit_checker.join()

class StandardInputRecordsReader(BaseThread):

    def __init__(self, stop_flag, queue, dry_run=False):
        super(StandardInputRecordsReader, self).__init__(stop_flag)
        self.queue = queue
        self.dry_run = dry_run

    def _run(self):
        while True:
            line = stdin.readline()
            #data = line.rstrip('\n')
            data = line
            if data:
                record = {'data': data}
                if self.dry_run:
                    stdout.write(str(record) + '\n')
                    stdout.flush()
                else:
                    self.queue.put(record)
            # EOF. Note that 'tail FILE' generates EOF
            # while 'tail -f FILE' doesn't.
            if not line:
                self.stop_flag.set()
                logger.debug('Reached the end')
            if self.stop_flag.is_set():
                logger.debug('Reader is leaving...')
                break


class RecordPublisher(BaseThread):

    MAX_RECORD_SIZE = 50 * 1024
    MAX_TIME_BETWEEN_PUTS = 5
    def __init__(
      self, 
      stop_flag, 
      queue, 
      kinesis_service, 
      stream_name, 
      partition_key, 
      batch_enabled, 
      push_delay):

        super(RecordPublisher, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_service = kinesis_service
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.batch_enabled = batch_enabled
        self.push_delay = push_delay
        self.sequence_number_for_ordering = None



    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        m = hashlib.md5()

        data = ''
        last_record_put_time = datetime.now()
        while True:
            try:
                queue_entry  = self.queue.get(False) 
                new_data = queue_entry['data']
                # if batching is turned off we immediately put the data
                if self.batch_enabled == False and len(data) > 0:
                    new_data = self._truncate_if_necessary(new_data, self.MAX_RECORD_SIZE)
                    new_data = new_data.rstrip('\n')
                    if self.partition_key is None: 
                       m = hashlib.md5()
                       m.update(new_data)
                       partition_key_to_use = m.hexdigest()
                    else
                       partition_key_to_use = self.partition_key
                    self._put_kinesis_record(partition_key_to_use, new_data)
                    continue
                logger.debug('New data: ' + new_data + '\n')
                if self._does_new_data_fit(new_data, data, self.MAX_RECORD_SIZE):
                    logger.debug('adding data to existing batch')
                    data += new_data
                    if self._is_time_to_put(last_record_put_time, self.MAX_TIME_BETWEEN_PUTS):
                        self.sequence_number_for_ordering = \
                            self._put_kinesis_record(self.partition_key, data)
                        data = ''
                        last_record_put_time = datetime.now()
                else:
                    self.sequence_number_for_ordering = \
                        self._put_kinesis_record(self.partition_key, data)
                    new_data = self._truncate_if_necessary(new_data, self.MAX_RECORD_SIZE)
                    data = ''
                    last_record_put_time = datetime.now()
            except Queue.Empty:
                if self.stop_flag.is_set():
                     logger.debug('Publisher is leaving...')
                     break
                else:
                    self.stop_flag.wait(5)
        # still need to put remaining records
        if len(data) > 0: 
            self._put_kinesis_record(self.partition_key, data)

    def _put_kinesis_record(self, partition_key, data):
        params = dict(StreamName=self.stream_name,
                      PartitionKey=partition_key,
                      Data=data)

        if self.sequence_number_for_ordering:
          params['SequenceNumberForOrdering'] = self.sequence_number_for_ordering
        
        try:
          response = self.kinesis_service.put_record(**params)
          stdout.write('.')
          stdout.flush()
          return response['SequenceNumber']
        except:
          type, value, traceback = exc_info()
          log_to_stderr('Caught exception while putting record to stream %s\n%s' 
                             % (self.stream_name, value))


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

