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

import functools
import sys
import time
import traceback


class ExponentialBackoff(object):

    """
    Decorator which performs exponential backoff and retry of a function for up
    to a maximum of max_retries everytime the function returns an exception.
    If max_retries is exceeded, raise an exception with the traceback of the
    last exception thrown.

    Customize using:
     * max_retries:
       set to the number of times to retry a failing method call
       Default: 5
     * logger:
       supply a logging.Logger object to send exception information to
       this stream
       Default: None
     * exception:
       to catch only a subset of exceptions. Use a tuple to catch
       multiple exceptions
       Default: None (catches all exceptions)
     * stderr:
       set to True to print exception information to sys.stderr.
       Default: False
     * initial_backoff:
       set the number of seconds for the first backoff.
       Default: 1s

    """

    def __init__(self, max_retries=5, logger=None, exception=Exception,
                 stderr=False, initial_backoff=1, quiet=False):
        self.max_retries = max_retries
        self.logger = logger
        self.exception = exception
        self.stderr = stderr
        self.quiet = quiet

        if initial_backoff <= 0:
            raise ValueError('initial_backoff must be larger than zero: %s' %
                             initial_backoff)
        else:
            self.initial_backoff = initial_backoff

    def __call__(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for retry in range(self.max_retries):
                try:
                    return f(*args, **kwargs)
                except self.exception:
                    # fall through to retry
                    pass

                # Requested logging
                backoff = self.initial_backoff * (2 ** retry)
                if retry < self.max_retries - 1:
                    msg = 'Method "%s" failed, backing off %s seconds, ' \
                          'and retrying' % (f.__name__, backoff)
                else:
                    msg = 'Method "%s" has failed for the last time.' \
                          % (f.__name__)
                if self.quiet:
                    pass
                else:
                    if self.logger:
                        self.logger.exception(msg)
                    if self.stderr:
                        sys.stderr.write('%s\n' % msg)

                # Sleep, except after the last failed attempt
                if retry < self.max_retries - 1:
                    time.sleep(backoff)
            else:
                # Dropped out of the while loop so we've exhausted all retries
                raise RuntimeError(
                    'Method "%s" has failed after %s unsuccessful '
                    'attempts.\n%s' % (f.__name__, self.max_retries,
                                       traceback.format_exc()))

        return wrapper
