# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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


class GetShardMetricsCommand(BasicCommand):
    NAME = 'get-shard-metrics'
    DESCRIPTION = 'Get Shard Metrics for a Kinesis Stream'
    ARG_TABLE = [
        {'name': 'stream-name', 'required': True, 'help_text': 'The name of the stream'}
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
        

    def _run_main(self, args, parsed_globals):

      kinesis_client = self.aws_generic_client('kinesis', parsed_globals)
      cloudwatch_client = self.aws_generic_client('cloudwatch', parsed_globals)
 
      shard_ids = self.get_shard_ids_for_stream(kinesis_client, args.stream_name)

      datapoints = {} 
      start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
      end_time = datetime.datetime.utcnow()

      for shard_id in shard_ids:
        response = cloudwatch_client.get_metric_statistics(
            Namespace = 'AWS/Kinesis',
            StartTime = start_time,
            EndTime = end_time,
            MetricName = 'IncomingRecords',
            Period = 60,
            Statistics = ['Sum'],
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
        entries = response['Datapoints']
        if len(entries) > 0:
          sums = map(lambda x: int(x['Sum']), entries)
          entries['avg'] = sum(sums) / len(sums)
          #entries['max'] = max(sums)
          print 'here'
          #print 'Shard id {0}: {1}, avg: {2}, max: {3}'.format(
          #  shard_id, 
          #  sums,
          #  entries['avg'],
          #  entries['max']
          #)
        else:
          print 'No data for shard id {0}'.format(shard_id)
        #datapoints[shard_id] = entries
      return 0


