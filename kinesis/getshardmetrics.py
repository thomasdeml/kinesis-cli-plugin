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
    DESCRIPTION = 'Get Shard Metrics for a Kinesis Stream'
    ARG_TABLE = [
        {'name': 'stream-name', 'required': True, 'help_text': 'The name of the stream'},
        {'name': 'metric', 'required': True, 'help_text': 'The name of the metric to query for. Allowed are "IncomingBytes", "IncomingRecords", "WriteProvisionedThroughputExceeded", "OutgoingBytes", "OutgoingRecords", "ReadProvisionedThroughputExceeded" and "IteratorAgeMilliseconds".'},
        {'name': 'statistic', 'required': True, 'help_text': 'The name of the statistic to query for. Allowed are "Average", "Sum", "SampleCount", "Minimum" and "Maximum".'},
        {'name': 'sort-by', 'required': False, 'help_text': 'The name of the calculation to sort by. Allowed are "Average", "Maximum" and "Minimum". Default is Average'},
        {'name': 'duration', 'required': False, 'help_text': 'The time duration to query for. Value must be between 1 and 30 minutes. Default is 10'},
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
 
    def get_shard_ids_for_stream(self, kinesis_client, stream_name):
      response = kinesis_client.describe_stream(
        StreamName = stream_name
      )
      #BUG BUG - do we need to handle > 100 shards, i.e. pagination!
      shard_ids = []
      for shard in response['StreamDescription']['Shards']:
        shard_ids.append(shard['ShardId'])
      return shard_ids
        
    def argument_validation(self, args):
      if args.duration is None or  args.duration < 1 or args.duration > 30:
         args.duration = 15
      else:
         args.duration = int(args.duration)
      return args 
    
    def metric_values(self, datapoints, statistic):
      return  map(lambda x: float(x[statistic]), datapoints)


    def _run_main(self, args, parsed_globals):
      args = self.argument_validation(args)
      kinesis_client = self.aws_generic_client('kinesis', parsed_globals)
      cloudwatch_client = self.aws_generic_client('cloudwatch', parsed_globals)
 
      shard_ids = self.get_shard_ids_for_stream(kinesis_client, args.stream_name)

      shard_metrics_array = []
      start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=args.duration)
      end_time = datetime.datetime.utcnow()

      for shard_id in shard_ids:
        response = cloudwatch_client.get_metric_statistics(
            Namespace = 'AWS/Kinesis',
            StartTime = start_time,
            EndTime = end_time,
            MetricName = args.metric,
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

      sorted_shard_array = [] 
      sorted_shard_array = sorted(
        shard_metrics_array, 
        key=lambda sma: sma.avg(),
        reverse=True
      )
      for sm in sorted_shard_array:
        if sm.has_data() > 0:
          print '{0}: avg: {1}, max: {2}'.format(
            sm.shard_id, 
            sm.avg(),
            sm.max()
          )
        else:
          print 'No data for shard id {0}'.format(shard_id)
      return 0


