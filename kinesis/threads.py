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
from threading import Thread
from sys import stderr


class ExitChecker(Thread):
    '''
    This thread periodically checks the stop_flag and leaves when
    stop_flag is set.
    '''

    def __init__(self, stop_flag):
        super(ExitChecker, self).__init__()
        self.daemon = True
        self.stop_flag = stop_flag

    def run(self):
        while True:
            if self.stop_flag.is_set():
                break
            else:
                self.stop_flag.wait(1)


class BaseThread(Thread):
    '''
    This thread should be extended by concrete thread which overrides
    the _run() method. If it exists accidentally, it sets the
    stop flag so other threads that check stop flag could exit as well.
    It's dangerous if either a producer or consumer thread dies, but
    not both.
    '''

    def __init__(self, stop_flag):
        super(BaseThread, self).__init__()
        self.daemon = True
        self.stop_flag = stop_flag

    def run(self):
        try:
            self._run()
        except Exception as e:
            msg = '%s leaving due to %s' % (self, e)
            stderr.write('%s\n' % msg)
            # If a thread exits accidentally, other threads should exit so
            # that client can observe the issue.
            if not self.stop_flag.is_set():
                self.stop_flag.set()

    def _run():
        raise NotImplementedError("_run")
