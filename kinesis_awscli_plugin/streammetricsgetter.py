from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics


class StreamMetricsGetter(object):
    def __init__(
            self,
            cloudwatch_helper,
            stream_name,
            start_time,
            end_time,
            statistic='Average',
            period=60, ):
        self.cloudwatch_helper = cloudwatch_helper
        self.stream_name = stream_name
        self.start_time = start_time
        self.end_time = end_time
        self.statistic = statistic
        self.period = period
        self.namespace = 'AWS/Kinesis'

    def get(self, metric_list):
        metrics_array = []
        for metric_name in metric_list:
            metrics_array.append(
                KinesisMetrics(
                    metric_name,
                    self.get_metric_datapoints(metric_name),
                    self.statistic, ))
        return metrics_array

    def get_metric_datapoints(self, metric_name):
        return self.cloudwatch_helper.get_metric_datapoints(
            Namespace=self.namespace,
            MetricName=metric_name,
            StartTime=self.start_time,
            EndTime=self.end_time,
            Statistics=[self.statistic],
            Period=self.period,
            Dimensions=self.cloudwatch_helper.get_stream_dimensions(
                self.stream_name))
