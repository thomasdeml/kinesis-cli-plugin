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
from kinesis_awscli_plugin.standardinputrecordsreader import StandardInputRecordsReader
from kinesis_awscli_plugin.recordpublisher import RecordPublisher
from kinesis_awscli_plugin.kinesishelper import KinesisHelper
from kinesis_awscli_plugin.utils \
     import log_to_stdout,log_to_stderr, register_ctrl_c_handler, example_text
import botocore
import botocore.exceptions
from six.moves import configparser
from sys import stdout
from awscli.errorhandler import ServerError

logger = logging.getLogger(__name__)


class PushCommand(BasicCommand):
    NAME = 'push'

    EXAMPLES = example_text(__file__, NAME + '.rst')

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
        self.kinesis = KinesisHelper(self._session, parsed_globals)
        register_ctrl_c_handler()
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
