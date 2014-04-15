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


import re
import string
from datetime import datetime, date

import _strptime


class DateTimeParser:
    """
    This parser passes datetime from a string according to the given
    datetime format. The string might contain datatime and extra characters.
    As long as the datetime string matches the given format, this parser
    should be able to extract it and parse it. The extra characters might
    slow down the parsing. The optional range_start and range_end parameters
    can be used if client knows the position of datetime string.
    """

    def __init__(self, format, range_start=None, range_end=None):
        self._format = format
        if format is not None:
            self._range_start = range_start
            self._range_end = range_end
            # TimeRE expands month format to lower case, so ignore case
            self._pattern = re.compile('.*?(%s).*' %
                                       _strptime.TimeRE().pattern(format),
                                       re.IGNORECASE)
            self._missing_year = not re.search('%[yY]', format)

    def parse(self, str):
        if self._format is None:
            return None
        if self._range_start is not None and self._range_end is not None:
            str = str[self._range_start:self._range_end]
        m = self._pattern.match(str)
        if m:
            dt = datetime.strptime(m.group(1), self._format)
            # If year is missing, use current year
            if self._missing_year:
                return dt.replace(year=date.today().year)
            else:
                return dt
        else:
            return None
