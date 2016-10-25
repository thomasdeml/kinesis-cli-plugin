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
from awscli.formatter import get_formatter
from kinesis_awscli_plugin.streammetricsgetter import StreamMetricsGetter
from kinesis_awscli_plugin.timestringconverter import TimeStringConverter

class GetStreamMetricsCommand(BasicCommand):

    NAME = 'get-stream-metrics'

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
      'Average',
      'Sum',
      'SampleCount',
      'Minimum',
      'Maximum'
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
                       'Default is now minus {1} minutes.'.format(
                          TimeStringConverter.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=DEFAULT_DURATION)), 
                          DEFAULT_DURATION
                       )
        },
        {
          'name': 'end-time', 
          'required': False, 
          'help_text': 'The end time for the metrics to query for in UTC. Time format is ISO8601. Example: "{0}". '\
                       'Default is "now".'.format(
                          TimeStringConverter.iso8601(datetime.datetime.utcnow())
                       )
        },
    ]


    def _run_main(self, args, parsed_globals):
      args = self.collect_args(args)
      self.validate_args(args)
      stream_metrics_array = []
      stream_metrics_getter = StreamMetricsGetter( 
        cloudwatch_client = self.aws_generic_client('cloudwatch', parsed_globals),
        kinesis_client = self.aws_generic_client('kinesis', parsed_globals),
	stream_name = args.stream_name,
	start_time = args.start_time,
	end_time = args.end_time,
	statistic = args.statistic,
      )
      stream_metrics = stream_metrics_getter.get(self.metric_names)
      stream_metrics_array.append(stream_metrics)
      output = self.create_stream_metrics_output(stream_metrics_array, args)
      self._display_response('get-stream-metrics', output, parsed_globals)
      return 0

    def collect_args(self, args):
      if args.start_time is None:
         args.start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=self.DEFAULT_DURATION)
      else: 
         args.start_time = datetime.datetime.strptime( args.start_time, "%Y-%m-%dT%H:%M:%S" )

      if args.metric_names is None: 
         args.metric_names = self.STREAM_METRIC_NAMES

      if args.statistic is None:
         args.statistic = 'Average'

      if args.end_time is None:
         args.end_time = datetime.datetime.utcnow()
      else:
         args.end_time = datetime.datetime.strptime( args.end_time, "%Y-%m-%dT%H:%M:%S" )
      return args

    def validate_args(self, args):
      for _metric in args.metric_names:
        if _metric not in self.STREAM_METRIC_NAMES:
          raise ValueError("{0} not found. Metric name must be one of the following: {1}".format(_metric, str(self.STREAM_METRIC_NAMES)))
     
      if args.statistic not in self.ALLOWED_STATISTICS: 
        raise ValueError('Statistic must be one of the following: {0}'.format(str(self.ALLOWED_STATISTICS)))
      
      if args.start_time > args.end_time:
         raise ValueError("Parameter start-time is newer than end-time")


    def aws_generic_client(self, service_name, globals):
      client = self.aws_client(
        service_name, 
        globals.region, 
        globals.endpoint_url, 
        globals.verify_ssl
      )
      return client
 
    def aws_client(self, service_name, region, endpoint_url, verify_ssl):
      client = self._session.create_client(
        service_name, 
        region_name=region,
        endpoint_url=endpoint_url,
        verify=verify_ssl
      )
      return client
     

    def create_stream_metrics_output(self, metrics_array, args):
      output = {}
      
      output['description'] = 'Stream metrics average of "{0}" for stream "{0}" between {1} and {2}'.format(
        args.statistic, 
        args.stream_name,
        TimeStringConverter.iso8601(args.start_time),
        TimeStringConverter.iso8601(args.end_time),
      )
      # args not json serializable. Need to do by hand
      output['start_time'] = TimeStringConverter.iso8601(args.start_time)
      output['end_time'] = TimeStringConverter.iso8601(args.end_time)
      output['metric_names'] = args.metric_names
      output['statistic'] = args.statistic 
 
      output['metrics'] = map(
        lambda _kinesis_metrics: {'Metric': _kinesis_metrics.metric_name, 'Average': round(_kinesis_metrics.avg()/60.0, 2), 'Datapoints': _kinesis_metrics.metric_values},
        metrics_array
      )
      return output

    def _display_response(self, command_name, response,
                          parsed_globals):
        output = parsed_globals.output
        if output is None:
            output = self._session.get_config_variable('output')
        formatter = get_formatter(output, parsed_globals)
        formatter(command_name, response)

