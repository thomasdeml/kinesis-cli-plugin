import logging
from threading import Thread, Event
from six.moves import queue as Queue

from awscli.customizations.commands import BasicCommand

from kinesis_awscli_plugin.lib.threads import ExitChecker
from kinesis_awscli_plugin.lib.recordrenderer import RecordRenderer
from kinesis_awscli_plugin.lib.recordspuller import RecordsPuller
from kinesis_awscli_plugin.lib.kinesishelper import KinesisHelper
from kinesis_awscli_plugin.lib.utils import Utils

logger = logging.getLogger(__name__)


class PullCommand(BasicCommand):

    NAME = 'pull'

    EXAMPLES = Utils.example_text(__file__, NAME + '.rst')

    DESCRIPTION = ('This command pulls records from a Kinesis stream. ')

    QUEUE_SIZE = 1000

    ARG_TABLE = [
        {
            'name': 'stream-name',
            'required': True,
            'help_text': 'Specifies the Kinesis stream name'
        },
        {
            'name': 'shard-id',
            'required': True,
            'help_text': 'Specifies the shard id that should be pulled.'
            'Can be retrieved via describe-stream'
        },
        {
            'name': 'pull-delay',
            'cli_type_name': 'integer',
            'default': '5000',
            'help_text':
            'Specifies the delay in milliseconds before pulling the '
            'next batch of records. Defaults to 5000 milliseconds.'
        },
        {
            'name': 'duration',
            'cli_type_name': 'integer',
            'default': '-1',
            'help_text':
            'Specifies how many seconds the command should pull from the stream. '
            'Defaults to -1 (infinite).'
        },
    ]

    def _run_main(self, args, parsed_globals):
        # Initialize services
        self.kinesis_helper = KinesisHelper(self._session, parsed_globals)
        # Run the command and report success
        self._call(args, parsed_globals)
        return 0

    def _call(self, options, parsed_globals):

        threads = []
        stop_flag = Event()
        shard_iterator = self.kinesis_helper.get_shard_iterator_from_latest(
            options.stream_name, options.shard_id)

        queue = Queue.Queue(self.QUEUE_SIZE)
        renderer = RecordRenderer(stop_flag, queue, options.pull_delay)
        renderer.start()
        threads.append(renderer)

        puller = RecordsPuller(
            stop_flag,
            queue,
            self.kinesis_helper.client,
            shard_iterator,
            int(options.pull_delay),
            int(options.duration), )
        puller.start()

        threads.append(puller)
        ExitChecker.wait_on_exit(stop_flag)
        for thread in threads:
            thread.join()
