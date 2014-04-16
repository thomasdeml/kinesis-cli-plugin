from threading import Thread, Event
import time
import base64
from datetime import datetime
import json
import logging
from sys import stdout, stderr
from dateutil.tz import tzlocal, tzutc
from six.moves import queue as Queue
from botocore.vendored import requests

from awscli.errorhandler import ServerError
from awscli.customizations.commands import BasicCommand
from awscli.customizations.service import Service
from awscli.customizations.kinesis.help import LogsDocHandler, LogsHelp
from awscli.customizations.kinesis import utils
from awscli.customizations.kinesis.retry import ExponentialBackoff
from awscli.customizations.kinesis.threads import BaseThread, ExitChecker
from awscli.customizations.kinesis.utils import endpoint_config

logger = logging.getLogger(__name__)

def initialize(cli):
  cli.register('building-command-table.kinesis', handler=inject_command)

def inject_command(command_table, session, **kwargs):
  command_table['pull'] = KinesisPull(session)
  
class KinesisPull(BasicCommand):
    NAME = 'pull'
    DESCRIPTION = ('This command pulls records from a Kinesis stream. ')
    SYNOPSIS = ''

    ARG_TABLE = [
        
        {'name': 'stream-name', 
         'required': True,
         'help_text': 'Specifies the Kinesis stream name'},
        
        {'name': 'shard-id', 
         'required': True, 
         'help_text': 'Specifies the shard id that should be pulled.'
                      'Can be retrieved via describe-stream'},
        
        {'name': 'pull-delay', 
         'cli_type_name': 'integer', 
         'default': '5',
         'help_text': 'Specifies the delay in seconds before pulling the '
                      'next batch of log events. Defaults to 5 seconds.'},
    ]

    UPDATE = False

    QUEUE_SIZE = 100

    def _run_main(self, args, parsed_globals):
        # Initialize services
        self.kinesis = Service(
            'kinesis', 
            endpoint_args=endpoint_config(parsed_globals),
            session=self._session)
        # Run the command and report success
        self._call(args, parsed_globals)

        return 0

    def _call(self, options, parsed_globals):

        params = dict(stream_name=options.stream_name,
                      shard_id=options.shard_id, 
                      shard_iterator_type = 'LATEST')
        gsi_response = self.kinesis.GetShardIterator(**params)

        threads = []
        stop_flag = Event()
        logger.debug(str(gsi_response))
        if gsi_response and gsi_response['ShardIterator']:
            queue = Queue.Queue(self.QUEUE_SIZE)
            renderer = EventsRenderer(stop_flag, queue)
            renderer.start()
            threads.append(renderer)
            puller = EventsPuller(
                stop_flag, 
                queue,
                self.kinesis,
                gsi_response['ShardIterator'],
                int(options.pull_delay))
            puller.start()
            threads.append(puller)
        else:
            print('Cannot retrieve shard iterator for stream [%s] / shard [%s] ' %
                  (options.stream_name, options.shard_id))

        self._wait_on_exit(stop_flag)
        for thread in threads:
            thread.join()

    def _wait_on_exit(self, stop_flag):
        exit_checker = ExitChecker(stop_flag)
        exit_checker.start()
        try:
            while exit_checker.is_alive() and not stop_flag.is_set():
                exit_checker.join(5)
        except KeyboardInterrupt:
            pass
        logger.debug('Setting stop_flag')
        stop_flag.set()
        exit_checker.join()


class EventsPuller(BaseThread):
    def __init__(self, stop_flag, queue, kinesis_service, shard_iterator,  pull_delay):
        super(EventsPuller, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_service = kinesis_service
        self.next_shard_iterator = shard_iterator
        self.pull_delay = pull_delay

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):

        while True:
            if self.stop_flag.is_set():
                logger.debug('Puller is leaving...')
                break
            else:
                self.stop_flag.wait(self.pull_delay)

            logger.debug('Getting records with shard iterator [%s]' %
                         (self.next_shard_iterator))

            params = dict(shard_iterator=self.next_shard_iterator)
            gr_response = self.kinesis_service.GetRecords(**params)
            if gr_response:
                records = gr_response['Records']
                if len(records) == 0:
                    logger.debug('No records read')
                else:
                    logger.debug('Adding records to the queue')
                    self.queue.put(EventBatch(records))
                    
                self.next_shard_iterator = gr_response['NextShardIterator']
            else:
               logger.debug('empty response')


class EventsRenderer(BaseThread):
    def __init__(self, stop_flag, queue):
        super(EventsRenderer, self).__init__(stop_flag)
        self.queue = queue

    def _run(self):
        while True:
            try:
                event_batch = self.queue.get(False)
                logger.debug('Rendering event batch. %d batches are remaining.' % self.queue.qsize())
                stdout.flush()
                for record in event_batch.records:
                    revised_record = record.copy()
                    stdout.write(base64.b64decode(revised_record['Data']) + '\n')
                    stdout.flush()
            except Queue.Empty:
                if self.stop_flag.is_set():
                    logger.debug('Renderer is leaving...')
                    break
                else:
                    logger.debug('waiting for more data')
                    self.stop_flag.wait(5)


class EventBatch:
    def __init__(self, records):
        self.records = records
