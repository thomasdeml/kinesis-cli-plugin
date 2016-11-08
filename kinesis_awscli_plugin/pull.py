from threading import Thread, Event
import time
import base64
import datetime
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
from kinesis_awscli_plugin.utils import example_text

logger = logging.getLogger(__name__)

class PullCommand(BasicCommand):
    NAME = 'pull'

    EXAMPLES = example_text(__file__, NAME + '.rst')

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

         {'name': 'duration', 
         'cli_type_name': 'integer', 
         'default': '-1',
         'help_text': 'Specifies how many seconds the command should pull from the stream. '
                      'Defaults to -1 (infinite).'},
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
            # BUGBUG: using pull delay also for rendering!!!
            renderer = RecordRenderer(stop_flag, queue, options.pull_delay)
            renderer.start()
            threads.append(renderer)
            puller = RecordsPuller(
                stop_flag, 
                queue,
                self.kinesis,
                gsi_response['ShardIterator'],
                int(options.pull_delay),
                int(options.duration),
            )
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
    def __init__(
      self, 
      stop_flag, 
      queue, 
      kinesis_service, 
      shard_iterator,  
      pull_delay,
      duration,
    ):
        super(RecordsPuller, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_service = kinesis_service
        self.next_shard_iterator = shard_iterator
        self.pull_delay = pull_delay
        self.duration = duration

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        if self.duration == -1:
            self.end_time = datetime.datetime(datetime.MAXYEAR,1,1)
        else:
            self.end_time = datetime.datetime.now() + datetime.timedelta(seconds = self.duration)

        logger.debug('pulling from stream ends at %s' %  self.end_time)

        while True:
            if datetime.datetime.now() > self.end_time:
               self.stop_flag.set()
            if self.stop_flag.is_set():
                logger.debug('Puller is leaving...')
                break
            else:
                #Event.wait expects wait time in seconds. Command-line uses milliseconds
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
    def __init__(self, stop_flag, queue, render_delay):
        super(RecordRenderer, self).__init__(stop_flag)
        self.queue = queue
        self.render_delay = render_delay

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
                    # wait expects time in seconds. Command-line passes it in milliseconds
                    self.stop_flag.wait(float(self.render_delay/1000.0))


class RecordBatch:
    def __init__(self, records):
        self.records = records
