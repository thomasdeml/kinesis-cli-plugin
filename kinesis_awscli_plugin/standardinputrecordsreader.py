import logging
import botocore
from sys import stdin, stderr, stdout
from kinesis_awscli_plugin.threads import BaseThread

logger = logging.getLogger(__name__)


class StandardInputRecordsReader(BaseThread):

    MAX_LINE_LENGTH = 20 * 1024

    def __init__(self, stop_flag, queue, dry_run=False):
        super(StandardInputRecordsReader, self).__init__(stop_flag)
        self.queue = queue
        self.dry_run = dry_run

    def _run(self):

        while True:
            line = stdin.readline()
            data = line
            if len(data) > self.MAX_LINE_LENGTH:
                raise ValueError("Line is too long")
            if data:
                record = {'data': data}
                if self.dry_run:
                    stdout.write(str(record) + '\n')
                    stdout.flush()
                else:
                    self.queue.put(record)
            # EOF. Note that 'tail FILE' generates EOF
            # while 'tail -f FILE' doesn't.
            if not line:
                self.stop_flag.set()
                logger.debug('Reached the end')
            if self.stop_flag.is_set():
                logger.debug('Reader is leaving...')
                break
