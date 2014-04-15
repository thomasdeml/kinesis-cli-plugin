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
import re
import socket
import six
from six.moves import queue as Queue
from six.moves import configparser
from botocore.vendored import requests

from awscli.errorhandler import ServerError
from awscli.customizations.commands import BasicCommand
from awscli.customizations.service import Service
from awscli.customizations.logs.parser import DateTimeParser
from awscli.customizations.logs import utils
from awscli.customizations.logs.utils import log_to_stdout,\
    log_to_stderr, get_current_millis, get_current_time_str
from awscli.customizations.logs.retry import ExponentialBackoff
from awscli.customizations.logs.threads import BaseThread, ExitChecker

logger = logging.getLogger(__name__)


def initialize(cli):
    """
    The entry point for Cloudwatch logs watch command.
    """
    cli.register('building-command-table.logs', inject_commands)


def inject_commands(command_table, session, **kwargs):
    """
    Called when the Cloudwatch logs command table is being built.
    Used to inject new high level commands into the command list.
    These high level commands must not collide with existing
    low-level API call names.
    """
    command_table['push'] = LogsPush(session)


class LogsPush(BasicCommand):
    """
    """
    NAME = 'push'
    DESCRIPTION = ('This command pushes log events to a log stream which is '
                   'specified by log group name and log stream name. The log '
                   'events can come from either standard input or files.')
    SYNOPSIS = ''
    DEFAULT_PUSH_DELAY = 1000

    ARG_TABLE = [
        {'name': 'config-file'},
        {'name': 'log-group-name'},
        {'name': 'log-stream-name'},
        {'name': 'push-delay', 'cli_type_name': 'integer',
         'default': DEFAULT_PUSH_DELAY,
         'help_text': 'Specifies the delay in milliseconds between publishing '
                      'two batches of log events. Defaults to 1000 ms.'},
        {'name': 'datetime-format',
         'help_text': 'Specifies how timestamp is extracted from logs. '
                      'The current time is used for each log event if it\'s '
                      'not provided. Below are some common formats: '
                      'Syslog: \'%b %d %H:%M:%S\', e.g. Jan 23 20:59:29'
                      'Log4j: \'%d %b %Y %H:%M:%S\', e.g. '
                      '24 Jan 2014 05:00:00'},
        {'name': 'datetime-range',
         'help_text': 'Optional value to improve the datetime parsing '
                      ' performance. Two numbers separated by ":" '
                      ' specifying the approximate start and end position '
                      ' of datetime string.'},
        {'name': 'dry-run', 'action': 'store_true',
         'help_text': 'Prints log events instead of sending to service.'},
    ]

    UPDATE = False
    QUEUE_SIZE = 10000

    def _run_main(self, args, parsed_globals):
        # Parse a dummy string to bypass a bug before using strptime in thread
        # https://bugs.launchpad.net/openobject-server/+bug/947231
        datetime.strptime('2012-01-01', '%Y-%m-%d')
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
        if args.config_file:
            self._call_push_file(args, parsed_globals)
        else:
            self._call_push_stdin(args, parsed_globals)

        return 0

    def _call_push_file(self, options, parsed_globals):
        threads = []
        stop_flag = Event()
        config = configparser.RawConfigParser()
        config.read(options.config_file)
        try:
            state_file = config.get(WatchConfig.GENERAL_SECTION,
                                    WatchConfig.STATE_FILE)
        except configparser.NoOptionError:
            state_file = WatchConfig.DEFAULT_STATE_FILE
        watcher = FileWatcher(stop_flag, self.logs, state_file,
                              options.dry_run)
        for section in config.sections():
            if section == WatchConfig.GENERAL_SECTION:
                continue
            watch_config = WatchConfig(config, section)
            watcher.register(watch_config)
        watcher.start()
        self._wait_on_exit(stop_flag)
        watcher.join()

    def _call_push_stdin(self, options, parsed_globals):
        dt_range_start, dt_range_end = (None, None)
        if options.datetime_range is not None:
            dt_range = options.datetime_range.split(':')
            try:
                dt_range_start = int(dt_range[0])
                dt_range_end = int(dt_range[1])
            except Exception:
                raise ValueError('You must pass two numbers separated by '
                                 '":" to --datetime-range')

        threads = []
        queue = Queue.Queue(self.QUEUE_SIZE)
        stop_flag = Event()
        try:
            reader = StandardInputEventsReader(stop_flag, queue,
                                               options.datetime_format,
                                               dt_range_start,
                                               dt_range_end,
                                               options.dry_run)
            reader.start()
        except KeyError as e:
            raise ValueError('You must pass valid datetime format to '
                             ' --datetime-format. "%s" is not valid.' % e)
        threads.append(reader)
        publisher = EventsPublisher(stop_flag, queue, self.logs,
                                    options.log_group_name,
                                    options.log_stream_name,
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


class WatchConfig:

    FILE = 'file'
    LOG_GROUP_NAME = 'log_group_name'
    LOG_STREAM_NAME = 'log_stream_name'
    DATETIME_FORMAT = 'datetime_format'
    PUSH_DELAY = 'push_delay'
    GENERAL_SECTION = 'general'
    STATE_FILE = 'state_file'
    DEFAULT_STATE_FILE = 'watchstate'
    DEFAULT_PUSH_DELAY = 150
    HOST_NAME_PLACEHOLDER = "::hostname::"
    HOST_NAME = socket.gethostname()

    def __init__(self, config, section):
        self.path_to_file = config.get(section, self.FILE)
        self.log_group_name = config.get(section, self.LOG_GROUP_NAME)
        self.log_stream_name = self._normalize(config.get(section,
                                               self.LOG_STREAM_NAME))
        self.dt_format = config.get(section, self.DATETIME_FORMAT)
        try:
            self.push_delay = config.getint(section, self.PUSH_DELAY)
        except configparser.NoOptionError:
            self.push_delay = self.DEFAULT_PUSH_DELAY

    def _normalize(self, stream_name):
        return stream_name.replace(self.HOST_NAME_PLACEHOLDER,
                                   self.HOST_NAME)


class FileWatcher(BaseThread):
    """
    A file might be rotated in different ways and new data will go to
    different places based on what ratation mechanism is used. This class
    detects the file rotation and starts a reader if needed.
    1) old file is renamed and a new file is created with same file name.
    2) old file is copied and then truncated.
    3) old file remains the same name, and a new file is created. Old and new
    file share a common pattern [Not supported yet].
    """
    QUEUE_SIZE = 1000

    def __init__(self, stop_flag, logs, state_file, dry_run=False):
        super(FileWatcher, self).__init__(stop_flag)
        self.uploaders = []
        self.logs = logs
        self.dry_run = dry_run
        self.state_file_lock = Lock()
        #TODO: problem happens if same state_file is updated by
        # different processes
        self.state_db = shelve.open(state_file)

    def register(self, config):
        self.uploaders.append(FileUploader(config))

    def _run(self):
        """
        For each file, calc its source_id based on the content and store it.
        if the source_id changes, it means the file has been rotated. The old
        reader is notified to exit once the end of file is reached. A new
        reader is started immediately to process the new file.
        """
        while True:
            if self.stop_flag.is_set():
                logger.debug('Watcher is leaving...')
                break
            else:
                self.stop_flag.wait(5)
            for uploader in self.uploaders:
                source_id = self._source_id(uploader.file)
                if source_id is None:
                    # no data yet, skip
                    continue
                if uploader.source_id != source_id:
                    # has data. Setup reader and publisher
                    if uploader.source_id is None:
                        # there is no publisher yet
                        logger.debug('Starting publisher for %s' % source_id)
                        queue = Queue.Queue(self.QUEUE_SIZE)
                        uploader.queue = queue
                        publisher = EventsPublisher(self.stop_flag,
                                                    uploader.queue, self.logs,
                                                    uploader.log_group_name,
                                                    uploader.log_stream_name,
                                                    uploader.push_delay)
                        publisher.register_publish_callback(
                            self._record_state)
                        publisher.start()
                        uploader.publisher = publisher
                    else:
                        # file has been rotated. Let reader exit.
                        logger.debug('File rotation detected.')
                        uploader.readers[uploader.source_id].exiting = True
                    # the file might be processed before, use last position
                    try:
                        last_position = self.state_db[source_id]
                    except KeyError:
                        last_position = 0
                    logger.debug('Starting reader for %s' % source_id)
                    reader = FileEventsReader(source_id,
                                              self.stop_flag,
                                              uploader.queue,
                                              uploader.dt_format,
                                              None, None, uploader.file,
                                              last_position,
                                              self.dry_run)
                    reader.start()
                    uploader.readers[source_id] = reader
                    uploader.source_id = source_id
                # clean up dead readers
                to_be_deleted = []
                for source_id in uploader.readers.keys():
                    if not uploader.readers[source_id].is_alive():
                        logger.debug('Removing dead reader %s' %
                                     source_id)
                        to_be_deleted.append(source_id)
                for source_id in to_be_deleted:
                    del uploader.readers[source_id]

        for uploader in self.uploaders:
            for reader in uploader.readers.values():
                reader.join()
            uploader.publisher.join()
        self.state_db.close()

    def _record_state(self, event):
        with self.state_file_lock:
            logger.debug('Saved %s:%s' % (event['source_id'],
                                          event['position']))
            self.state_db[event['source_id']] = event['position']
            self.state_db.sync()

    def _source_id(self, file):
        # Return None instead of throwing exception if the log file
        # doesn't exist.
        while not os.path.isfile(file):
            log_to_stdout('Target file %s does not exist.'
                          ' Nothing to do.\n' % (file))
            return None

        with open(file, 'r') as f:
            str = f.readline()
            if str:
                return hashlib.md5(str).hexdigest()
        return None


class FileUploader:
    def __init__(self, config):
        self.file = config.path_to_file
        self.log_group_name = config.log_group_name
        self.log_stream_name = config.log_stream_name
        self.dt_format = config.dt_format
        self.push_delay = config.push_delay
        self.queue = None
        self.readers = dict()
        self.publisher = None
        self.source_id = None


class FileEventsReader(BaseThread):

    def __init__(self, source_id, stop_flag, queue, dt_format,
                 dt_range_start, dt_range_end, file, offset, dry_run):
        super(FileEventsReader, self).__init__(stop_flag)
        self.source_id = source_id
        self.queue = queue
        self.dry_run = dry_run
        self.parser = DateTimeParser(dt_format, dt_range_start, dt_range_end)
        self.dt_format = dt_format
        self._validate_file(file)
        self.file = file
        self.offset = offset
        self.exiting = False
        self.open = open
        self.last_event = None

    def _push_last_event(self):
        if self.last_event:
            if self.dry_run:
                stdout.write(str(self.last_event) + '\n')
                stdout.flush()
            else:
                self.queue.put(self.last_event)
            self.last_event = None

    def _starts_with_whitespace(self, message):
        ws_matcher = re.compile("^\s+")
        if ws_matcher.search(message):
            return True
        return False

    def _run(self):
        previous_datetime = None
        with self.open(self.file, 'r') as fo:
            fo.seek(self.offset)
            while True:
                if self.stop_flag.is_set():
                    logger.debug('Reader is leaving as requested...')
                    self._push_last_event()
                    break
                curr_position = fo.tell()
                line = fo.readline()
                if not line:
                    self._push_last_event()
                    if self.exiting:
                        logger.debug('No data is left. Reader is leaving')
                        return
                    fo.seek(curr_position)
                    self.stop_flag.wait(1)
                else:
                    message = line.rstrip('\n')
                    if self.last_event and\
                       self._starts_with_whitespace(message):
                        self.last_event['message'] += "\n" + message
                    else:
                        self._push_last_event()
                        if self.dt_format:
                            curr_datetime = self.parser.parse(message)
                        else:
                            curr_datetime = datetime.utcnow()
                        if curr_datetime is None:
                            if previous_datetime is None:
                                log_to_stderr('Failed to detect datetime '
                                              'from %s. \n' % message)
                                continue
                            else:
                                curr_datetime = previous_datetime
                        else:
                            previous_datetime = curr_datetime
                        event = {'timestamp': utils.epoch(curr_datetime),
                                 'message': message,
                                 'position': fo.tell(),
                                 'source_id': self.source_id}
                        self.last_event = event

    def _validate_file(self, file):
        if not os.access(file, os.F_OK):
            raise ValueError("File '%s' does not exist" % (file))
        if not os.access(file, os.R_OK):
            raise ValueError("File '%s' is not readable" % (file))
        if not os.path.isfile(file):
            raise ValueError("File '%s' is not a file" % (file))


class StandardInputEventsReader(BaseThread):

    def __init__(self, stop_flag, queue, dt_format,
                 dt_range_start, dt_range_end, dry_run):
        super(StandardInputEventsReader, self).__init__(stop_flag)
        self.queue = queue
        self.dry_run = dry_run
        self.dt_format = dt_format
        self.parser = DateTimeParser(dt_format, dt_range_start, dt_range_end)

    def _run(self):
        previous_datetime = None
        while True:
            line = stdin.readline()
            message = line.rstrip('\n')
            # the service doesn't allow empty message so exclude it here
            if message:
                if self.dt_format:
                    curr_datetime = self.parser.parse(message)
                else:
                    curr_datetime = datetime.utcnow()
                if curr_datetime is None:
                    if previous_datetime is None:
                        stderr.write('Failed to detect datetime from line'
                                     '. Please verify your data.\n')
                        continue
                    else:
                        curr_datetime = previous_datetime
                else:
                    previous_datetime = curr_datetime
                event = {'timestamp': utils.epoch(curr_datetime),
                         'message': message}
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


class EventsPublisher(BaseThread):

    PER_EVENT_OVERHEAD = 26
    MAX_BATCH_SIZE = 32 * 1024
    DEFAULT_NUM_OF_EVENTS_PER_BATCH = 1000

    MILLIS_PER_HOUR = 60 * 60 * 1000

    # All events in a batch must belong to a 24hrs period.
    MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR

    # CWL allows posting events up to two hours in future
    MAX_ALLOWED_FUTURE_EVENT_TIME = 2 * MILLIS_PER_HOUR

    # CWL doesn't allow backfill more than 14 days in past.
    MAX_ALLOWED_BACKFILL_INTERVAL = 14 * MILLIS_PER_DAY

    def __init__(self, stop_flag, queue, logs_service, log_group_name,
                 log_stream_name, push_delay,
                 num_of_events_per_batch=DEFAULT_NUM_OF_EVENTS_PER_BATCH,
                 max_batch_size=MAX_BATCH_SIZE,
                 per_event_overhead=PER_EVENT_OVERHEAD,
                 max_time_range_per_batch=MILLIS_PER_DAY,
                 max_allowed_future_event_time=MAX_ALLOWED_FUTURE_EVENT_TIME,
                 max_allowed_backfill_interval=MAX_ALLOWED_BACKFILL_INTERVAL):
        super(EventsPublisher, self).__init__(stop_flag)
        self.queue = queue
        self.logs_service = logs_service
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name
        self.push_delay = push_delay
        self.publish_callback = None
        self.num_of_events_per_batch = num_of_events_per_batch
        self.max_batch_size = max_batch_size
        self.per_event_overhead = per_event_overhead
        self.batch = 0
        self.events = []
        self.last_save_time = None
        self.first_event_timestamp = None
        self.last_event = None
        self.force_save = False
        self.batch_size_in_bytes = 0
        self.last_dequeued_event = None
        self.max_time_range_per_batch = max_time_range_per_batch
        self.max_allowed_future_event_time = max_allowed_future_event_time
        self.max_allowed_backfill_interval = max_allowed_backfill_interval
        self.push_now = False

    def register_publish_callback(self, publish_callback):
        """
        The publish_callback is called with last published event
        when a successful publish is made
        """
        self.publish_callback = publish_callback

    def _get_event_size(self, event):
        return len(event['message']) + self.per_event_overhead

    def _truncate_event(self, event):
        truncated = "[TRUNCATED]"
        event['message'] = (event['message'][:(self.max_batch_size
                                               - self.per_event_overhead
                                               - len(truncated))]
                            + truncated)

    def _update_batch_size(self, event):
        self.batch_size_in_bytes += (len(event['message']) +
                                     self.per_event_overhead)

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        sequence_token = self._get_sequence_token()
        if self.last_save_time is None:
            self.last_save_time = get_current_millis()

        while True:
            try:
                # Only dequeue if we don't already have a de-queued event.
                if self.last_dequeued_event is None:
                    self.last_dequeued_event = self.queue.get(False)

                event = self.last_dequeued_event

                # Ignore blank messages or they'll cause PutLogEvents to fail.
                if event['message'].strip() == '':
                    self.last_dequeued_event = None
                    continue

                # Drop the event if its too far in future.
                if event['timestamp'] > (get_current_millis() +
                                         self.max_allowed_future_event_time):
                    log_to_stdout('Event too far in future. Dropping it.'
                                  '{timestamp = %d, message = %s}' %
                                  (event['timestamp'], event['message']))
                    self.last_dequeued_event = None
                    continue

                # Drop the event if its too far in the past.
                if event['timestamp'] < (get_current_millis() -
                                         self.max_allowed_backfill_interval):
                    log_to_stdout('Event too far in past. Dropping it.'
                                  '{timestamp = %d, message = %s}' %
                                  (event['timestamp'], event['message']))
                    self.last_dequeued_event = None
                    continue

                # Save the timestamp for the first event in the batch.
                if self.first_event_timestamp is None:
                    self.first_event_timestamp = event['timestamp']

                # Truncate the event if it is bigger than the max allowed
                # batch size.
                if self._get_event_size(event) > self.max_batch_size:
                    log_to_stdout('Very large event detected. Truncating it to'
                                  ' %d  bytes. Event timestamp: %d ' %
                                  (self.max_batch_size, event['timestamp']))

                    self._truncate_event(event)
                    self.last_dequeued_event = event

                new_length = (self.batch_size_in_bytes +
                              self._get_event_size(event))

                # Event's time-range for this batch
                time_range = event['timestamp'] - self.first_event_timestamp

                if len(self.events) < self.num_of_events_per_batch and \
                   new_length <= self.max_batch_size and \
                   time_range < self.max_time_range_per_batch:

                    self.last_event = event
                    self._update_batch_size(event)
                    self.events.append(
                        {'timestamp': event['timestamp'],
                         'message': event['message']})
                    self.last_dequeued_event = None
                else:
                    # We've reached the max possible events for this batch.
                    self.push_now = True
            except Queue.Empty:
                if self.stop_flag.is_set():
                    if not self.events:
                        logger.debug('Publisher is leaving...')
                        break
                    else:
                        self.force_save = True
                else:
                    self.stop_flag.wait(5)

            if len(self.events) > 0:
                delta = self.push_delay - (get_current_millis() -
                                           self.last_save_time)
                if len(self.events) >= self.num_of_events_per_batch or \
                        delta <= 0 or self.force_save or self.push_now:
                    # Delay enough time before publish
                    if delta > 0:
                        self.stop_flag.wait(delta / float(1000))
                    sequence_token = self._put_log_events(self.events,
                                                          sequence_token)
                    if self.publish_callback:
                        self.publish_callback(self.last_event)
                    self.first_event_timestamp = None
                    self.events = []
                    self.last_save_time = get_current_millis()
                    self.push_now = False
            else:
                self.last_save_time = get_current_millis()

    def _get_sequence_token(self):
        log_to_stdout('Creating log-stream %s\n' % self.log_stream_name)
        try:
            self.logs_service.CreateLogStream(
                log_group_name=self.log_group_name,
                log_stream_name=self.log_stream_name)
        except:
            pass

        log_streams_resp = self.logs_service.DescribeLogStreams(
            log_group_name=self.log_group_name,
            log_stream_name_prefix=self.log_stream_name)
        if log_streams_resp and log_streams_resp['logStreams']:
            return log_streams_resp['logStreams'][0].get('uploadSequenceToken')
        else:
            return None

    def _put_log_events(self, events, sequence_token):

        conflicting_op_re = "A conflicting operation is currently in progress"
        bad_sequence_token_re = "with sequenceToken: (\d+)"

        # In one batch, events should be sorted in ascending order
        events = sorted(events, key=itemgetter('timestamp'))
        params = dict(log_group_name=self.log_group_name,
                      log_stream_name=self.log_stream_name,
                      log_events=events)

        while True:
            if sequence_token:
                params['sequence_token'] = sequence_token

            try:
                response = self.logs_service.PutLogEvents(**params)
                break
            except:
                type, value, traceback = exc_info()
                log_to_stderr('Caught an exception: %s\n' % value)

                # Retry if its due to a conflicting operation.
                match = re.search(conflicting_op_re, str(value))

                if match:
                    continue

                bad_token_match = re.search(bad_sequence_token_re, str(value))

                if bad_token_match:
                    sequence_token = bad_token_match.group(1)
                    log_to_stdout('Found sequence token: %s. Retrying\n' %
                                  sequence_token)
                    continue
                else:
                    raise

        log_to_stdout('LogGroup: %s LogStream: %s -- Published %d events, '
                      'BatchId: %d, SizeInBytes: %d\n' %
                      (self.log_group_name,
                       self.log_stream_name,
                       len(events),
                       self.batch,
                       self.batch_size_in_bytes))
        self.batch += 1
        self.batch_size_in_bytes = 0
        return response['nextSequenceToken']
