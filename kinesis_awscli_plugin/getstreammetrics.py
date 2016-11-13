# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import json
import os
import sys
import datetime

from awscli.customizations.commands import BasicCommand
from kinesis_awscli_plugin.lib.streammetricsgetter import StreamMetricsGetter
from kinesis_awscli_plugin.lib.timeutils import TimeUtils
from kinesis_awscli_plugin.lib.cloudwatchhelper import CloudWatchHelper
from kinesis_awscli_plugin.lib.utils import example_text, display_response


class GetStreamMetricsCommand(BasicCommand):

    NAME = 'get-stream-metrics'

    EXAMPLES = example_text(__file__, NAME + '.rst')

    DESCRIPTION = """The command gets Stream Metrics for the specified Kinesis Stream."""

    STREAM_METRIC_NAMES = [
        'IncomingBytes',
        'IncomingRecords',
        'PutRecord.Bytes',
        'PutRecord.Latency',
        'PutRecord.Success',
        'PutRecords.Bytes',
        'PutRecords.Latency',
        'PutRecords.Records',
        'PutRecords.Success',
        'ReadProvisionedThroughputExceeded',
        'WriteProvisionedThroughputExceeded',
        'GetRecords.Bytes',
        'GetRecords.IteratorAge',
        'GetRecords.IteratorAgeMilliseconds',
        'GetRecords.Latency',
        'GetRecords.Records',
        'GetRecords.Success',
    ]

    ALLOWED_STATISTICS = [
        'Average', 'Sum', 'SampleCount', 'Minimum', 'Maximum'
    ]

    DEFAULT_DURATION = 10

    ARG_TABLE = [
        {
          'name': 'stream-name',
          'required': True,
          'help_text': 'The name of the stream'
        },
        {
          'name': 'metric-names',
          'required': False,
          'help_text': 'A list of metrics to query for. By default all stream-level metrics are queried: '\
          .format(str(STREAM_METRIC_NAMES))
        },
        {
          'name': 'statistic',
          'required': False,
          'help_text': 'The name of the statistic to query for. Default is "Average".'\
            'Allowed are: {0}'.format(str(ALLOWED_STATISTICS))
        },
        {
          'name': 'start-time',
          'required': False,
          'help_text': 'The start time for the metrics to query for in UTC. Time format is ISO8601. Example: "{0}". '\
                       'Default is now minus {1} minutes. Relative times like "30 minutes ago" can be used if the '\
                       'Python module dateparser is installed (pip install dateparser).'.format(
                          TimeUtils.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=DEFAULT_DURATION)),
                          DEFAULT_DURATION
                       )
        },
        {
          'name': 'end-time',
          'required': False,
          'help_text': 'The end time for the metrics to query for in UTC. Time format is ISO8601. Example: "{0}". '\
                       'Default is "now". Relative times like "30 minutes ago" can be used if the '\
                       'Python module dateparser is installed (pip install dateparser).'.format(
                          TimeUtils.iso8601(datetime.datetime.utcnow())
                       )
        },
    ]

    def _run_main(self, args, parsed_globals):
        args = self.collect_args(args)
        self.validate_args(args)
        stream_metrics_array = []
        stream_metrics_getter = StreamMetricsGetter(
            cloudwatch_helper=CloudWatchHelper(self._session, parsed_globals),
            stream_name=args.stream_name,
            start_time=args.start_time,
            end_time=args.end_time,
            statistic=args.statistic, )
        stream_metrics = stream_metrics_getter.get(args.metric_names)
        output = self.create_stream_metrics_output(stream_metrics, args)
        display_response(self._session, 'get-stream-metrics', output,
                         parsed_globals)
        return 0

    def collect_args(self, args):
        if args.start_time is None:
            args.start_time = datetime.datetime.utcnow() - datetime.timedelta(
                minutes=self.DEFAULT_DURATION)
        else:
            args.start_time = TimeUtils.to_datetime(args.start_time)

        if args.metric_names is None:
            args.metric_names = self.STREAM_METRIC_NAMES
        else:
            args.metric_names = args.metric_names.split(',')

        if args.statistic is None:
            args.statistic = 'Average'

        if args.end_time is None:
            args.end_time = datetime.datetime.utcnow()
        else:
            args.end_time = TimeUtils.to_datetime(args.end_time)
        return args

    def validate_args(self, args):
        for _metric in args.metric_names:
            if _metric not in self.STREAM_METRIC_NAMES:
                raise ValueError(
                    "{0} not found. Metric name must be one of the following: {1}".
                    format(_metric, str(self.STREAM_METRIC_NAMES)))

        if args.statistic not in self.ALLOWED_STATISTICS:
            raise ValueError('Statistic must be one of the following: {0}'.
                             format(str(self.ALLOWED_STATISTICS)))

        if args.start_time > args.end_time:
            raise ValueError("Parameter start-time is newer than end-time")

    def create_stream_metrics_output(self, metrics_array, args):
        output = {}

        output[
            'Description'] = 'Stream metrics for stream "{1}" between {2} and {3}'.format(
                args.statistic,
                args.stream_name,
                TimeUtils.iso8601(args.start_time),
                TimeUtils.iso8601(args.end_time), )
        # args not json serializable. Need to do by hand
        output['StartTime'] = TimeUtils.iso8601(args.start_time)
        output['EndTime'] = TimeUtils.iso8601(args.end_time)
        output['MetricNames'] = args.metric_names
        output['Statistic'] = args.statistic

        output['Metrics'] = map(
          lambda _kinesis_metrics: {'Metric': _kinesis_metrics.metric_id, 'Datapoints': _kinesis_metrics.datapoints},
          metrics_array
        )
        return output
