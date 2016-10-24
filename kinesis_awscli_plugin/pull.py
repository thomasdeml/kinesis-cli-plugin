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
#from awscli.customizations.kinesis import utils
from kinesis_awscli_plugin.retry import ExponentialBackoff
from kinesis_awscli_plugin.threads import BaseThread, ExitChecker

logger = logging.getLogger(__name__)

class PullCommand(BasicCommand):
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
         'default': '5000',
         'help_text': 'Specifies the delay in milliseconds before pulling the '
                      'next batch of records. Defaults to 5000 milliseconds.'},
    ]

    UPDATE = False

    QUEUE_SIZE = 10

    def _run_main(self, args, parsed_globals):
        # Initialize services
        self.kinesis = self._session.create_client(
            'kinesis', 
            region_name=parsed_globals.region,
            endpoint_url = parsed_globals.endpoint_url,
            verify = parsed_globals.verify_ssl
        )
        # Run the command and report success
        self._call(args, parsed_globals)

        return 0

    def _call(self, options, parsed_globals):

        params = dict(StreamName=options.stream_name,
                      ShardId=options.shard_id, 
                      ShardIteratorType = 'LATEST')
        gsi_response = self.kinesis.get_shard_iterator(**params)

        threads = []
        stop_flag = Event()
        logger.debug(str(gsi_response))
        if gsi_response and gsi_response['ShardIterator']:
            queue = Queue.Queue(self.QUEUE_SIZE)
            renderer = RecordRenderer(stop_flag, queue)
            renderer.start()
            threads.append(renderer)
            puller = RecordsPuller(
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


class RecordsPuller(BaseThread):
    def __init__(self, stop_flag, queue, kinesis_service, shard_iterator,  pull_delay):
        super(RecordsPuller, self).__init__(stop_flag)
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
                #Event.wait expects wait time in milliseconds
                self.stop_flag.wait(float(self.pull_delay/1000.0))

            logger.debug('Getting records with shard iterator [%s]' %
                         (self.next_shard_iterator))

            params = dict(ShardIterator=self.next_shard_iterator)
            gr_response = self.kinesis_service.get_records(**params)
            if gr_response:
                records = gr_response['Records']
                if len(records) == 0:
                    logger.debug('No records read')
                else:
                    logger.debug('Adding records to the queue')
                    self.queue.put(RecordBatch(records))
                    
                self.next_shard_iterator = gr_response['NextShardIterator']
            else:
               logger.debug('empty response')


class RecordRenderer(BaseThread):
    def __init__(self, stop_flag, queue):
        super(RecordRenderer, self).__init__(stop_flag)
        self.queue = queue

    def _run(self):
        while True:
            try:
                record_batch = self.queue.get(False)
                logger.debug('Rendering record batch. %d batches are remaining.' % self.queue.qsize())
                stdout.flush()
                for record in record_batch.records:
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


class RecordBatch:
    def __init__(self, records):
        self.records = records
