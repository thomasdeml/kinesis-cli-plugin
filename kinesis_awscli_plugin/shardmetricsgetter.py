from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics


class ShardMetricsGetter(object):
    def __init__(
            self,
            cloudwatch_helper,
            kinesis_helper,
            stream_name,
            start_time,
            end_time,
            metric_name='IncomingRecords',
            statistic='Average',
            period=60, ):
        self.cloudwatch_helper = cloudwatch_helper
        self.kinesis_helper = kinesis_helper
        self.stream_name = stream_name
        self.start_time = start_time
        self.end_time = end_time
        self.metric_name = metric_name
        self.statistic = statistic
        self.period = period
        self.namespace = 'AWS/Kinesis'

    def get(self):
        shard_ids = self.get_shard_ids_for_stream()
        shard_metrics_array = self.get_shard_metrics(shard_ids)
        return self.sort(shard_metrics_array)

    def get_shard_datapoints(self, shard_id):
        return self.cloudwatch_helper.get_metric_datapoints(
            Namespace=self.namespace,
            MetricName=self.metric_name,
            StartTime=self.start_time,
            EndTime=self.end_time,
            Statistics=[self.statistic],
            Period=self.period,
            Dimensions=self.cloudwatch_helper.get_shard_dimensions(
                self.stream_name,
                shard_id, ))

    def get_shard_ids_for_stream(self):
        return self.kinesis_helper.stream_shards(self.stream_name)

    def get_shard_metrics(self, shard_ids):
        shard_metrics_array = []
        for shard_id in shard_ids:
            shard_metrics_array.append(
                KinesisMetrics(shard_id,
                               self.get_shard_datapoints(shard_id),
                               self.statistic))
        return shard_metrics_array

    def sort(self, shard_metrics_array):
        return sorted(
            shard_metrics_array,
            key=lambda _shard_metrics_array: _shard_metrics_array.datapoint_average,
            reverse=True)
