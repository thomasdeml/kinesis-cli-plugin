from threading import Thread, Event
import logging
from six.moves import queue as Queue
from awscli.customizations.commands import BasicCommand
from kinesis_awscli_plugin.threads import ExitChecker
from kinesis_awscli_plugin.utils import example_text
from kinesis_awscli_plugin.recordrenderer import RecordRenderer
from kinesis_awscli_plugin.recordspuller import RecordsPuller

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


