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


from sys import stdin, stderr, stdout
from datetime import datetime
import time

from dateutil.tz import *

ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def epoch(dt, tzinfo=None):
    delta = (dt - datetime(1970, 1, 1, tzinfo=tzinfo))
    return delta.days*24*3600*1000 + delta.seconds*1000 +\
        delta.microseconds//1000


def utc_epoch(utc_dt):
    return epoch(utc_dt, tzutc())


def iso8601_to_epoch(utc_dt_str):
    dt = datetime.strptime(utc_dt_str, ISO8601_FORMAT)
    dt = dt.replace(tzinfo=tzutc())
    return utc_epoch(dt)


def get_current_millis():
    return int(time.time() * 1000)


def get_current_time_str():
    fmt = '%Y-%m-%d %H:%M:%S'
    return datetime.fromtimestamp(time.time()).strftime(fmt)


def log_to_stdout(line):
    stdout.write(get_current_time_str() + ' -- ' + line)
    stdout.flush()


def log_to_stderr(line):
    stderr.write(get_current_time_str() + ' -- ' + line)
    stderr.flush()


def endpoint_config(global_args):
    endpoint_args = {
       'region_name': None,
       'endpoint_url': None
    }
    if 'region' in global_args:
        endpoint_args['region_name'] = global_args.region
    if 'endpoint_url' in global_args:
        endpoint_args['endpoint_url'] = global_args.endpoint_url
    return endpoint_args   
