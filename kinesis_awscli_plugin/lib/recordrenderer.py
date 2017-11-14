import logging
import base64
from six.moves import queue as Queue
from sys import stdout

from kinesis_awscli_plugin.lib.threads import BaseThread, ExitChecker

logger = logging.getLogger(__name__)


class RecordRenderer(BaseThread):
    def __init__(self, stop_flag, queue, render_delay):
        super(RecordRenderer, self).__init__(stop_flag)
        self.queue = queue
        self.render_delay = render_delay

    def _run(self):
        while True:
            try:
                record_batch = self.queue.get(False)
                logger.debug(
                    'Rendering record batch. %d batches are remaining.' %
                    self.queue.qsize())
                for record in record_batch.records:
                    revised_record = record.copy()
                    stdout.write(
                        base64.b64decode(revised_record['Data']).decode('utf-8') + '\n')
                    stdout.flush()
            except Queue.Empty:
                if self.stop_flag.is_set():
                    logger.debug('Renderer is leaving...')
                    break
                else:
                    logger.debug('waiting for more data')
                    # wait expects time in seconds. Command-line passes it in milliseconds
                    self.stop_flag.wait(float(self.render_delay / 1000.0))
