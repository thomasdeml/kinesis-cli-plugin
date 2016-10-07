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

    def aws_client(self, service_name, region, endpoint_url, verify_ssl):
      client = self._session.create_client(
        service_name, 
        region_name=region,
        endpoint_url=endpoint_url,
        verify=verify_ssl
      )
      return client
 
    def get_stream_shards(self, kinesis_client, stream_name):
      return 0

    def _run_main(self, args, parsed_globals):
      cloudwatch_client = self.aws_client('cloudwatch', parsed_globals.region, parsed_globals.endpoint_url, parsed_globals.verify_ssl)
      kinesis_client = self.aws_client('kinesis', parsed_globals.region, parsed_globals.endpoint_url, parsed_globals.verify_ssl)
       
      stream_shards = self.get_stream_shards(kinesis_client, args.stream_name)

      response = cloudwatch_client.get_metric_statistics(
          Namespace = 'AWS/Kinesis',
          StartTime = datetime.datetime.utcnow() - datetime.timedelta(minutes=15),
          EndTime = datetime.datetime.utcnow(),
          MetricName = 'IncomingRecords',
          Period = 60,
          Statistics = ['Sum'],
          Dimensions=[
            {
              'Name': 'StreamName',
              'Value': args.stream_name
            },
          ],
      )
      sys.stdout.write(
        str(response['Datapoints'])
      )
      return 0


