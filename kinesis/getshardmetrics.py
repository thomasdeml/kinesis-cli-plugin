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
import contextlib
import os
import sys
import datetime

from awscli.customizations.commands import BasicCommand
from awscli.customizations.kinesis.shardmetrics import ShardMetrics

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
          'required': True, 
          'help_text': 'The name of the metric to query for. Allowed '\
            'are {0}:'.format(str(ALLOWED_METRIC_NAMES))
        },
        {
          'name': 'statistic', 
          'required': True, 
          'help_text': 'The name of the statistic to query for. '\
            'Allowed are: {0}'.format(str(ALLOWED_STATISTICS))
        },
        {
          'name': 'duration', 
          'required': False, 
          'help_text': 'The time duration to query for. Value must '\
            'be between 1 and 30 minutes. Default is {0}'.format(str(DEFAULT_DURATION))
        },
    ]


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

 
    def get_shard_ids_for_stream(self, stream_name):
      response = self.kinesis_client.describe_stream(
        StreamName = stream_name
      )
      #BUG BUG - do we need to handle > 100 shards, i.e. pagination!
      shard_ids = []
      for shard in response['StreamDescription']['Shards']:
        shard_ids.append(shard['ShardId'])
      return shard_ids

        
    def argument_validation(self, args):
     if args.metric_name not in self.ALLOWED_METRIC_NAMES:
       raise ValueError('Metric name must be one of these: {0}'.format(str(self.ALLOWED_METRIC_NAMES)))
     
     if args.statistic not in self.ALLOWED_STATISTICS: 
       raise ValueError('Statistic must be one of these: {0}'.format(str(self.ALLOWED_STATISTICS)))

     if args.duration is None or  args.duration < 1 or args.duration > 30:
       args.duration = self.DEFAULT_DURATION
     else:
       args.duration = int(args.duration)
     return args 

    
    def metric_values(self, datapoints, statistic):
      return  map(lambda x: float(x[statistic]), datapoints)


    def get_metrics_for_shards(self, shard_ids, args):
      shard_metrics_array = []
      
      start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=args.duration)
      end_time = datetime.datetime.utcnow()

      for shard_id in shard_ids:
        response = self.cloudwatch_client.get_metric_statistics(
            Namespace = 'AWS/Kinesis',
            StartTime = start_time,
            EndTime = end_time,
            MetricName = args.metric_name,
            Period = 60,
            Statistics = [args.statistic],
            Dimensions=[
              {
                'Name': 'StreamName',
                'Value': args.stream_name
              },
              {
                'Name': 'ShardId',
                'Value': shard_id
              },
            ],
        )
        values = self.metric_values(
          response['Datapoints'],
          args.statistic
        )
        shard_metrics_array.append(
          ShardMetrics(
            shard_id, 
            values, 
          )
        )
      return shard_metrics_array


    def print_shard_metrics(self, sorted_shard_array, args):
      print 'Average of "{0} (statistic {1}) per second" over the last {2} minutes:'.format(
        args.metric_name, 
        args.statistic,
        args.duration,
      )
        
      for shard_metrics in sorted_shard_array:
        if shard_metrics.has_data() > 0:
          print '{0}: avg: {1:15.2f}'.format(
            shard_metrics.shard_id, 
            shard_metrics.avg()/60.0,
          )
        else:
          print 'No data for shard id {0}'.format(shard_metrics.shard_id)


    def _run_main(self, args, parsed_globals):
      args = self.argument_validation(args)
      self.kinesis_client = self.aws_generic_client('kinesis', parsed_globals)
      self.cloudwatch_client = self.aws_generic_client('cloudwatch', parsed_globals)
 
      shard_ids = self.get_shard_ids_for_stream(args.stream_name)

      shard_metrics_array = self.get_metrics_for_shards(shard_ids, args)

      sorted_shard_array = sorted(
        shard_metrics_array, 
        key=lambda sma: sma.avg(),
        reverse=True
      )
      self.print_shard_metrics(sorted_shard_array, args)
      return 0
