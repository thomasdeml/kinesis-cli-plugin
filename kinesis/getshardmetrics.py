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


from awscli.customizations.commands import BasicCommand


class GetShardMetricsCommand(BasicCommand):
    NAME = 'get-shard-metrics'
    DESCRIPTION = 'Get Shard Metrics for a Kinesis Stream'
    ARG_TABLE = [
        {'name': 'stream-name', 'required': True, 'help_text': 'The name of the stream'}
    ]

    def _run_main(self, args, parsed_globals):
        cloudwatch_client = self._session.create_client(
            'cloudwatch', region_name=parsed_globals.region,
            endpoint_url=parsed_globals.endpoint_url,
            verify=parsed_globals.verify_ssl
        )
        sys.stdout.write(
            'Test\n'
            'Stream Name: %s\n' % (args.stream_name))

        #response = cloudwatch_client.list_metrics()
        return 0
