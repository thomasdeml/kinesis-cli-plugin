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
from kinesis_awscli_plugin.shardmetricsgetter import ShardMetricsGetter
from kinesis_awscli_plugin.timestringconverter import TimeStringConverter

class GetShardMetricsCommand(BasicCommand):

    NAME = 'get-shard-metrics'

    DESCRIPTION = """Get Shard Metrics for a Kinesis Stream. The metrics 
      are sorted using the average over the specified duration as the 
      sort key."""

    ALLOWED_METRIC_NAMES = [
      'IncomingBytes', 
      'IncomingRecords', 
      'WriteProvisionedThroughputExceeded', 
      'OutgoingBytes',
      'OutgoingRecords', 
      'ReadProvisionedThroughputExceeded',
      'IteratorAgeMilliseconds'
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
          'name': 'metric-name', 
          'required': False, 
          'help_text': 'The name of the metric to query for. Default is "IncomingRecords". Allowed '\
            'are {0}:'.format(str(ALLOWED_METRIC_NAMES))
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
      shard_metrics_getter = ShardMetricsGetter( 
        cloudwatch_client = self.aws_generic_client('cloudwatch', parsed_globals),
        kinesis_client = self.aws_generic_client('kinesis', parsed_globals),
        stream_name = args.stream_name,
        metric_name = args.metric_name,
        start_time = args.start_time,
        end_time = args.end_time,
        statistic = args.statistic,
      )
      shard_metrics = shard_metrics_getter.get()
 
      self.print_shard_metrics(shard_metrics, args)
      return 0


    def collect_args(self, args):
      if args.start_time is None:
         args.start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=self.DEFAULT_DURATION)
      else: 
         args.start_time = datetime.datetime.strptime( args.start_time, "%Y-%m-%dT%H:%M:%S" )

      if args.metric_name is None: 
         args.metric_name = 'IncomingRecords'

      if args.statistic is None:
         args.statistic = 'Average'

      if args.end_time is None:
         args.end_time = datetime.datetime.utcnow()
      else:
         args.end_time = datetime.datetime.strptime( args.end_time, "%Y-%m-%dT%H:%M:%S" )
      return args

    def validate_args(self, args):
      if args.metric_name not in self.ALLOWED_METRIC_NAMES:
        raise ValueError('Metric name must be one of the following: {0}'.format(str(self.ALLOWED_METRIC_NAMES)))
     
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
     

    def print_shard_metrics(self, sorted_shard_array, args):
      output = {}
      
      output['description'] = 'Average of "{0} ({1}) per second" between {2} and {3}'.format(
        args.metric_name, 
        args.statistic,
        TimeStringConverter.iso8601(args.start_time),
        TimeStringConverter.iso8601(args.end_time),
      )
      # args not json serializable. Need to do by hand
      output['start_time'] = TimeStringConverter.iso8601(args.start_time)
      output['end_time'] = TimeStringConverter.iso8601(args.end_time)
      output['metric_name'] = args.metric_name
      output['statistic'] = args.statistic 
 
      output['shard_metrics'] = map(
        lambda _shard: {_shard.shard_id: {'average': round(_shard.avg()/60.0, 2), 'datapoints': _shard.metric_values},
        sorted_shard_array
      )
      print json.dumps(
        output,
        indent = 1, 
        separators=(',', ': ')
      )
