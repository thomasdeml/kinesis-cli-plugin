# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from threading import Thread, Event
import time
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
from awscli.customizations.logs.help import LogsDocHandler, LogsHelp
from awscli.customizations.logs import utils
from awscli.customizations.logs.retry import ExponentialBackoff
from awscli.customizations.logs.threads import BaseThread, ExitChecker

logger = logging.getLogger(__name__)


def initialize(cli):
    """
    The entry point for Cloudwatch logs pull command.
    """
    cli.register('building-command-table.logs', inject_commands)


def inject_commands(command_table, session, **kwargs):
    """
    Called when the Cloudwatch logs command table is being built.
    Used to inject new high level commands into the command list.
    These high level commands must not collide with existing
    low-level API call names.
    """
    command_table['pull'] = LogsPull(session)


class LogsPull(BasicCommand):
    """
    """
    NAME = 'pull'
    DESCRIPTION = ('This command pulls log events from log stream(s). '
                   'If log-group-name and log-stream-name-prefix identify '
                   'one log stream, then it pulls log events from that one '
                   'stream. Otherwise, it pulls log events from multiple log '
                   'streams.')
    SYNOPSIS = ''

    ARG_TABLE = [
        {'name': 'log-group-name', 'required': True},
        {'name': 'log-stream-name-prefix'},
        {'name': 'start-time',
         'help_text': 'Optional value to specify start time of log events. '
                      'Default value is current time. The value has to '
                      'be in ISO8601 format (YYYY-MM-DDThh:mm:ssZ). '
                      'e.g. 2013-12-23T14:01:00Z'},
        {'name': 'end-time',
         'help_text': 'Optional value to specify end time of log events. '
                      'The value has to be in ISO8601 format '
                      '(YYYY-MM-DDThh:mm:ssZ). e.g. 2013-12-23T14:01:00Z'},
        {'name': 'suppress-header', 'action': 'store_true',
         'help_text': 'Suppresses printing of log stream name. By default '
                      'log stream name is printed.'},
        {'name': 'output-format', 'default': '{timestamp} {message}',
         'help_text': 'Optional value to specify the output format '
                      'of log events. Defaults to "{timestamp} {message}". '
                      'Valid keys are timestamp, message and ingestionTime.'},
        {'name': 'follow', 'action': 'store_true',
         'help_text': 'Not to stop when the end of the log stream is reached, '
                      'but to wait for additional log events to be appended, '
                      'until end time is reached.'},
        {'name': 'pull-delay', 'cli_type_name': 'integer', 'default': '5',
         'help_text': 'Specifies the delay in seconds before pulling the '
                      'next batch of log events. Defaults to 5 seconds.'},
    ]

    UPDATE = False

    QUEUE_SIZE = 10

    def create_help_command(self):
        '''
        Overrides the parent class to create a custom help command.
        '''
        return LogsHelp(self._session, self, command_table={},
                        arg_table=self.arg_table,
                        event_handler_class=LogsDocHandler)

    def _run_main(self, args, parsed_globals):
        endpoint_args = {
            'region_name': None,
            'endpoint_url': None
        }
        if 'region' in parsed_globals:
            endpoint_args['region_name'] = parsed_globals.region
        if 'endpoint_url' in parsed_globals:
            endpoint_args['endpoint_url'] = parsed_globals.endpoint_url

        # Initialize services
        self.logs = Service('logs', endpoint_args=endpoint_args,
                            session=self._session)
        # Run the command and report success
        self._call(args, parsed_globals)

        return 0

    def _call(self, options, parsed_globals):
        if options.start_time:
            try:
                start_time_in_ms = utils.iso8601_to_epoch(options.start_time)
            except ValueError as e:
                raise ValueError('%s. You must pass a valid start time to '
                                 ' --start-time' % e)
        else:
            start_time_in_ms = None

        if options.follow and start_time_in_ms is None:
            start_time_in_ms = utils.utc_epoch(
                datetime.utcnow().replace(tzinfo=tzutc()))

        if options.end_time:
            try:
                end_time_in_ms = utils.iso8601_to_epoch(options.end_time)
            except ValueError as e:
                raise ValueError('%s. You must pass a valid end time to '
                                 ' --end-time' % e)
        else:
            end_time_in_ms = None

        params = dict(log_group_name=options.log_group_name)
        if options.log_stream_name_prefix:
            params['log_stream_name_prefix'] = options.log_stream_name_prefix
        log_streams_response = self.logs.DescribeLogStreams(**params)

        threads = []
        stop_flag = Event()
        if log_streams_response and log_streams_response['logStreams']:
            queue = Queue.Queue(self.QUEUE_SIZE)
            renderer = EventsRenderer(stop_flag, queue,
                                      options.output_format,
                                      options.suppress_header)
            renderer.start()
            threads.append(renderer)
            for log_stream in log_streams_response['logStreams']:
                puller = EventsPuller(stop_flag, queue,
                                      self.logs,
                                      options.log_group_name,
                                      log_stream['logStreamName'],
                                      start_time_in_ms,
                                      end_time_in_ms,
                                      options.follow,
                                      int(options.pull_delay))
                puller.start()
                threads.append(puller)
        else:
            print('Log group [%s] has no log streams' %
                  options.log_group_name)

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
        logger.debug('Shutting down...')
        stop_flag.set()
        exit_checker.join()


class EventsPuller(BaseThread):
    def __init__(self, stop_flag, queue, logs_service, log_group_name,
                 log_stream_name, start_time, end_time, follow, pull_delay):
        super(EventsPuller, self).__init__(stop_flag)
        self.queue = queue
        self.logs_service = logs_service
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.start_time = start_time
        self.end_time = end_time
        self.follow = follow
        self.pull_delay = pull_delay
        self.next_token = None

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        params = dict(log_group_name=self.log_group_name,
                      log_stream_name=self.log_stream_name,
                      start_from_head=True)

        while True:
            if self.stop_flag.is_set():
                logger.debug('Puller is leaving...')
                break
            else:
                self.stop_flag.wait(self.pull_delay)

            logger.debug('Pulling log events with [%s] [%s] [%s]' %
                         (self.log_group_name,
                          self.log_stream_name,
                          self.next_token))

            if self.next_token:
                params['next_token'] = self.next_token
            if self.start_time:
                params['start_time'] = self.start_time
            if self.end_time:
                params['end_time'] = self.end_time
            log_events_response = self.logs_service.GetLogEvents(**params)
            if log_events_response and log_events_response['events']:
                logger.debug('Adding log events to the queue')
                self.next_token = log_events_response['nextForwardToken']
                self.queue.put(EventBatch(self.log_group_name,
                                          self.log_stream_name,
                                          log_events_response['events']))
            else:
                if not self.follow:
                    logger.debug('Pulled all log events')
                    self.stop_flag.set()


class EventsRenderer(BaseThread):
    def __init__(self, stop_flag, queue, output_format,
                 suppress_header=False):
        super(EventsRenderer, self).__init__(stop_flag)
        self.queue = queue
        self.output_format = u''.join(output_format)
        self.suppress_header = suppress_header

    def _run(self):
        while True:
            try:
                event_batch = self.queue.get(False)
                logger.debug('Rendering event batch. %d batches are remaining.'
                             % self.queue.qsize())
                if not self.suppress_header:
                    stdout.write('==> %s/%s <==\n' %
                                 (event_batch.log_group_name,
                                  event_batch.log_stream_name))
                    stdout.flush()
                for event in event_batch.events:
                    revised_event = event.copy()
                    if event.get('timestamp') is not None:
                        revised_event['timestamp'] = \
                            datetime.fromtimestamp(event['timestamp'] / 1000.0,
                                                   tzlocal())
                    if event.get('ingestionTime') is not None:
                        revised_event['ingestionTime'] = \
                            datetime.fromtimestamp(
                                event['ingestionTime'] / 1000.0,
                                tzlocal())
                    stdout.write(
                        self.output_format.format(**revised_event))
                    stdout.write('\n')
                    stdout.flush()
            except Queue.Empty:
                if self.stop_flag.is_set():
                    logger.debug('Renderer is leaving...')
                    break
                else:
                    self.stop_flag.wait(5)


class EventBatch:
    def __init__(self, log_group_name, log_stream_name, events):
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.events = events
