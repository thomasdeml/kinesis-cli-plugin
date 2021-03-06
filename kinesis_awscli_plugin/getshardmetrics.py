import json
import os
import sys
import datetime

from awscli.customizations.commands import BasicCommand

from kinesis_awscli_plugin.lib.shardmetricsgetter import ShardMetricsGetter
from kinesis_awscli_plugin.lib.timeutils import TimeUtils
from kinesis_awscli_plugin.lib.kinesishelper import KinesisHelper
from kinesis_awscli_plugin.lib.cloudwatchhelper import CloudWatchHelper
from kinesis_awscli_plugin.lib.utils import Utils


class GetShardMetricsCommand(BasicCommand):

    NAME = 'get-shard-metrics'

    EXAMPLES = Utils.example_text(__file__, NAME + '.rst')

    DESCRIPTION = """Get Shard Metrics for a Kinesis Stream. The metrics 
      are sorted using the average over the specified duration as the 
      sort key."""

    ALLOWED_METRIC_NAMES = [
        'IncomingBytes', 'IncomingRecords',
        'WriteProvisionedThroughputExceeded', 'OutgoingBytes',
        'OutgoingRecords', 'ReadProvisionedThroughputExceeded',
        'IteratorAgeMilliseconds'
    ]

    ALLOWED_STATISTICS = [
        'Average', 'Sum', 'SampleCount', 'Minimum', 'Maximum'
    ]

    DEFAULT_DURATION = 10

    ARG_TABLE = [
        {
          'name': 'stream-name',
          'required': True,
          'help_text': 'The name of the stream'
        },
        {
          'name': 'metric-name',
          'required': False,
          'help_text': 'The name of the metric to query for. Default is "IncomingRecords". Allowed '\
            'are {0}'.format(str(ALLOWED_METRIC_NAMES))
        },
        {
          'name': 'statistic',
          'required': False,
          'help_text': 'The name of the statistic to query for. Default is "Average". '\
            'Allowed are {0}'.format(str(ALLOWED_STATISTICS))
        },
        {
          'name': 'start-time',
          'required': False,
          'help_text': 'The start time for the metrics to query for in UTC. Time format is ISO8601. Example: "{0}". '\
                       'Default is now minus {1} minutes. Relative times like "30 minutes ago" can be used if the '\
                       'Python module dateparser is installed. '.format(
                          TimeUtils.iso8601(datetime.datetime.utcnow() - datetime.timedelta(minutes=DEFAULT_DURATION)),
                          DEFAULT_DURATION
                       )
        },
        {
          'name': 'end-time',
          'required': False,
          'help_text': 'The end time for the metrics to query for in UTC. Time format is ISO8601. Example: "{0}". '\
                       'Default is "now". Relative times like "30 minutes ago" can be used if the '\
                       'Python module dateparser is installed. '.format(
                          TimeUtils.iso8601(datetime.datetime.utcnow())
                       )
        },
    ]

    def _run_main(self, args, parsed_globals):
        args = self.collect_args(args)
        self.validate_args(args)
        self.cloudwatch_helper = CloudWatchHelper(self._session,
                                                  parsed_globals)
        self.kinesis_helper = KinesisHelper(self._session, parsed_globals)
        if self.kinesis_helper.shard_metrics_enabled(
                args.stream_name) == False:
            raise ValueError(
                "Shard Metrics are not enabled for this stream. Use the command enable-enhanced-monitoring to enable them."
            )
        shard_metrics_getter = ShardMetricsGetter(
            cloudwatch_helper=self.cloudwatch_helper,
            kinesis_helper=self.kinesis_helper,
            stream_name=args.stream_name,
            metric_name=args.metric_name,
            start_time=args.start_time,
            end_time=args.end_time,
            statistic=args.statistic, )
        shard_metrics = shard_metrics_getter.get()

        output = self.create_shard_metrics_output(shard_metrics, args)
        Utils.display_response(self._session, 'get-shard-metrics', output,
                         parsed_globals)
        return 0

    def collect_args(self, args):
        if args.start_time is None:
            args.start_time = datetime.datetime.utcnow() - datetime.timedelta(
                minutes=self.DEFAULT_DURATION)
        else:
            args.start_time = TimeUtils.to_datetime(args.start_time)

        if args.metric_name is None:
            args.metric_name = 'IncomingRecords'

        if args.statistic is None:
            args.statistic = 'Average'

        if args.end_time is None:
            args.end_time = datetime.datetime.utcnow()
        else:
            args.end_time = TimeUtils.to_datetime(args.end_time)
        return args

    def validate_args(self, args):
        if args.metric_name not in self.ALLOWED_METRIC_NAMES:
            raise ValueError('Metric name must be one of the following: {0}'.
                             format(str(self.ALLOWED_METRIC_NAMES)))

        if args.statistic not in self.ALLOWED_STATISTICS:
            raise ValueError('Statistic must be one of the following: {0}'.
                             format(str(self.ALLOWED_STATISTICS)))

        if args.start_time > args.end_time:
            raise ValueError("Parameter start-time is newer than end-time")

    def create_shard_metrics_output(self, sorted_shard_array, args):
        output = {}

        output['Description'] = "Sorted datapoint average of '{0}' ({1}) per minute between {2} and {3}".format(
                args.metric_name,
                args.statistic,
                TimeUtils.iso8601(args.start_time),
                TimeUtils.iso8601(args.end_time), )
        # args not json serializable. Need to do by hand
        output['StartTime'] = TimeUtils.iso8601(args.start_time)
        output['EndTime'] = TimeUtils.iso8601(args.end_time)
        output['MetricName'] = args.metric_name
        output['Statistic'] = args.statistic

        output['ShardMetrics'] = map(
          lambda _shard: {'ShardId': _shard.metric_id, 'DatapointAverage': round(_shard.datapoint_average, 2), 'Datapoints': _shard.datapoints},
          sorted_shard_array
        )
        return output
