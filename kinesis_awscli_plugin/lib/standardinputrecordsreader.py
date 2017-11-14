import logging
import botocore
from sys import stdin, stderr, stdout

from kinesis_awscli_plugin.lib.threads import BaseThread

logger = logging.getLogger(__name__)


class StandardInputRecordsReader(BaseThread):

    MAX_LINE_LENGTH = 20 * 1024

    def __init__(self, stop_flag, queue, dry_run=False):
        super(StandardInputRecordsReader, self).__init__(stop_flag)
        self.queue = queue
        self.dry_run = dry_run

    def _run(self):
        while True:
            line = self.read_stdin_line()
            if line:
                if len(line) > self.MAX_LINE_LENGTH:
                   logger.info("The following line is too long (it's not pushed): " + line)
                record = {'data': line}
                if self.dry_run:
                    self.write_to_stdout_and_flush(record)
                else:
                    self.queue.put(record)
            # EOF. Note that 'tail FILE' generates EOF
            # while 'tail -f FILE' doesn't.
            else:
                self.stop_flag.set()
                logger.debug('Reached the end')
            if self.stop_flag.is_set():
                logger.debug('Reader is leaving...')
                break

    def read_stdin_line(self):
        return stdin.readline()

    def write_stdout_and_flush(self, record):
        stdout.write(str(record) + '\n')
        stdout.flush()
