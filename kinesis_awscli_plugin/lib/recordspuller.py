import logging
import datetime

from awscli.errorhandler import ServerError

from kinesis_awscli_plugin.lib.retry import ExponentialBackoff
from kinesis_awscli_plugin.lib.threads import BaseThread

logger = logging.getLogger(__name__)


class RecordsPuller(BaseThread):
    def __init__(
            self,
            stop_flag,
            queue,
            kinesis_service,
            shard_iterator,
            pull_delay,
            duration, ):
        super(RecordsPuller, self).__init__(stop_flag)
        self.queue = queue
        self.kinesis_service = kinesis_service
        self.next_shard_iterator = shard_iterator
        self.pull_delay = pull_delay
        self.duration = duration

    @ExponentialBackoff(stderr=True, logger=logger, exception=(ServerError))
    def _run(self):
        if self.duration == -1:
            self.end_time = datetime.datetime(datetime.MAXYEAR, 1, 1)
        else:
            self.end_time = datetime.datetime.now() + datetime.timedelta(
                seconds=self.duration)

        logger.debug('pulling from stream ends at %s' % self.end_time)

        while True:
            if datetime.datetime.now() > self.end_time:
                self.stop_flag.set()
            if self.stop_flag.is_set():
                logger.debug('Puller is leaving...')
                break
            else:
                #Event.wait expects wait time in seconds. Command-line uses milliseconds
                self.stop_flag.wait(float(self.pull_delay / 1000.0))

            logger.debug('Getting records with shard iterator [%s]' %
                         (self.next_shard_iterator))

            params = dict(ShardIterator=self.next_shard_iterator)
            gr_response = self.kinesis_service.get_records(**params)
            if gr_response:
                records = gr_response['Records']
                if len(records) == 0:
                    logger.debug('No records read')
                else:
                    logger.debug('Adding records to the queue')
                    self.queue.put(RecordBatch(records))

                self.next_shard_iterator = gr_response['NextShardIterator']
            else:
                logger.debug('empty response')


class RecordBatch:
    def __init__(self, records):
        self.records = records
