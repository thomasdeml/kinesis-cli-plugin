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

import signal
import os
import sys
from awscli.formatter import get_formatter


def ctrl_c_handler(signum, frame):
    print("\nYou hit Ctrl+C.\nExiting ")
    exit()


def register_ctrl_c_handler():
    signal.signal(signal.SIGINT, ctrl_c_handler)


def example_text(module_path, example_file):
    module_path = os.path.dirname(os.path.abspath(module_path))
    example_file_path = os.path.join(module_path,
                                     'examples/kinesis/' + example_file)
    return open(example_file_path, 'r').read()


def display_response(session, command_name, response, parsed_globals):
    output = parsed_globals.output
    if output is None:
        output = session.get_config_variable('output')
    formatter = get_formatter(output, parsed_globals)
    formatter(command_name, response)
