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
from awscli.customizations.kinesis.retry import ExponentialBackoff
from awscli.customizations.kinesis.utils import log_to_stdout,log_to_stderr
from awscli.errorhandler import ServerError
from awscli.customizations.commands import BasicCommand
from awscli.customizations.service import Service
from awscli.customizations.kinesis.threads import BaseThread, ExitChecker
import botocore
import botocore.exceptions
from botocore.config import get_config  
from six.moves import configparser
from sys import stdout
from awscli.errorhandler import ServerError
from awscli.customizations.commands import BasicCommand
from awscli.customizations.service import Service

logger = logging.getLogger(__name__)

def initialize(cli):
  cli.register('building-command-table.kinesis', handler=inject_command)

def inject_command(command_table, session, **kwargs):
  command_table['push'] = KinesisPush(session)


class KinesisPush(BasicCommand):
    NAME = 'push'
    DESCRIPTION = ('This command pushes streams to a Kinesis stream.  '
                   'Events can come from either standard input or files.')
    SYNOPSIS = ''
    DEFAULT_PUSH_DELAY = 1000

    ARG_TABLE = [
        {'name': 'stream-name'},
        {'name': 'partition-key'},
        {'name': 'push-delay', 'cli_type_name': 'integer',
         'default': DEFAULT_PUSH_DELAY,
         'help_text': 'Specifies the delay in milliseconds between publishing '
                      'two batches of streams. Defaults to 1000 ms.'},
        {'name': 'dry-run', 'action': 'store_true',
         'help_text': 'Prints stream data instead of sending to service.'},
    ]

    UPDATE = False
    QUEUE_SIZE = 10000


    def _endpoint_args(self, global_args):
        endpoint_args = {
            'region_name': None,
            'endpoint_url': None
        }
        if 'region' in global_args:
            endpoint_args['region_name'] = global_args.region
        if 'endpoint_url' in global_args:
            endpoint_args['endpoint_url'] = global_args.endpoint_url
        return endpoint_args   


    def _run_main(self, args, parsed_globals):
        endpoint_args = self._endpoint_args(parsed_globals)
        self.kinesis = Service('kinesis', 
                              endpoint_args=endpoint_args,
                              session=self._session)
        self._call_push_stdin(args, parsed_globals)
        return 0

    def _call_push_stdin(self, options, parsed_globals):
        threads = []
        queue = Queue.Queue(self.QUEUE_SIZE)
        stop_flag = Event()
        reader = StandardInputEventsReader(stop_flag, queue)
        reader.start()
        threads.append(reader)
        publisher = EventPublisher(stop_flag, queue, self.kinesis, options.stream_name, options.partition_key, int(options.push_delay))
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

class StandardInputEventsReader(BaseThread):

    def __init__(self, stop_flag, queue, dry_run=False):
        super(StandardInputEventsReader, self).__init__(stop_flag)
        self.queue = queue
        self.dry_run = dry_run

    def _run(self):
        while True:
            line = stdin.readline()
            message = line.rstrip('\n')
            if message:
                event = {'message': message}
                if self.dry_run:
                    stdout.write(str(event) + '\n')
                    stdout.flush()
                else:
                    self.queue.put(event)
            # EOF. Note that 'tail FILE' generates EOF
            # while 'tail -f FILE' doesn't.
            if not line:
                self.stop_flag.set()
                logger.debug('Reached the end')
            if self.stop_flag.is_set():
                logger.debug('Reader is leaving...')
                break


class EventPublisher(BaseThread):

    MAX_RECORD_SIZE = 50 * 1024

    def __init__(self, stop_flag, queue, kinesis_service, stream_name, partition_key, push_delay):
        super(EventPublisher, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_service = kinesis_service
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.push_delay = push_delay
        self.sequence_number_for_ordering = None

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        while True:
            try:
                event = self.queue.get(False) 
                message = event['message']
                logger.debug('Message: ' + message)
                if len(message) > self.MAX_RECORD_SIZE:
                    log_to_stdout('Very large record detected. Truncating it to'
                                  ' %d  bytes.' %
                                  (self.MAX_RECORD_SIZE))
                    event['message'] = message[:self.MAX_RECORD_SIZE]
                self.sequence_number_for_ordering = self._put_kinesis_record(event)

            except Queue.Empty:
                if self.stop_flag.is_set():
                     logger.debug('Publisher is leaving...')
                     break
                else:
                    self.stop_flag.wait(5)

    def _put_kinesis_record(self, event):
        params = dict(stream_name=self.stream_name,
                      partition_key=self.partition_key,
                      data=event['message'])

        if self.sequence_number_for_ordering:
          params['sequence_number_for_ordering'] = self.sequence_number_for_ordering
        
        try:
          response = self.kinesis_service.PutRecord(**params)
          stdout.write('.')
          stdout.flush()
          return response['SequenceNumber']
        except:
          type, value, traceback = exc_info()
          log_to_stderr('Caught exception while putting record to stream %s\n%s' 
                             % (self.stream_name, value))
