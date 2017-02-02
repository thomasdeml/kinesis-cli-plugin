import logging
from sys import exc_info
from threading import Event
from six.moves import queue as Queue

from awscli.customizations.commands import BasicCommand

from kinesis_awscli_plugin.lib.threads import BaseThread, ExitChecker
from kinesis_awscli_plugin.lib.retry import ExponentialBackoff
from kinesis_awscli_plugin.lib.standardinputrecordsreader import StandardInputRecordsReader
from kinesis_awscli_plugin.lib.recordpublisher import RecordPublisher
from kinesis_awscli_plugin.lib.kinesishelper import KinesisHelper
from kinesis_awscli_plugin.lib.utils import Utils

logger = logging.getLogger(__name__)


class PushCommand(BasicCommand):
    NAME = 'push'

    EXAMPLES = Utils.example_text(__file__, NAME + '.rst')

    DESCRIPTION = ('This command pushes records to a Kinesis stream.  '
                   'Standard input is read line by line and sent as record. '
                   'A partition key has to be specified as well.')
    SYNOPSIS = ''
    DEFAULT_PUSH_DELAY = 1000

    ARG_TABLE = [
        {
            'name': 'stream-name',
            'required': True,
            'help_text': 'Specifies the stream name'
        },
        {
            'name': 'partition-key',
            'required': False,
            'help_text':
            'Specifies the partition key, e.g. $HOSTNAME. If not provided the partition key is random number.'
        },
        {
            'name': 'push-delay',
            'cli_type_name': 'integer',
            'default': DEFAULT_PUSH_DELAY,
            'help_text':
            'Specifies the delay in milliseconds between publishing '
            'two batches of streams. Defaults to 1000 ms. Records also '
            'get put if the maximum payload of 50kB is reached.'
        },
        {
            'name': 'disable-batch',
            'action': 'store_true',
            'help_text':
            'Batches are batched up to 50k payload. Specify --_-batch to disable batching.'
        },
        {
            'name': 'dry-run',
            'action': 'store_true',
            'help_text': 'Prints stream data instead of sending to service.'
        },
    ]

    UPDATE = False
    QUEUE_SIZE = 10000

    def _run_main(self, args, parsed_globals):
        self.kinesis_helper = KinesisHelper(self._session, parsed_globals)
        Utils.register_ctrl_c_handler()
        self._call_push_stdin(args, parsed_globals)
        return 0

    def _call_push_stdin(self, options, parsed_globals):
        threads = []
        queue = Queue.Queue(self.QUEUE_SIZE)
        stop_flag = Event()
        reader = StandardInputRecordsReader(stop_flag, queue, options.dry_run)
        reader.start()
        threads.append(reader)
        publisher = RecordPublisher(stop_flag, queue, self.kinesis_helper,
                                    options.stream_name, options.partition_key,
                                    options.disable_batch,
                                    int(options.push_delay))
        publisher.start()
        threads.append(publisher)
        ExitChecker.wait_on_exit(stop_flag)
        reader.join()
        publisher.join()
