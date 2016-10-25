from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics

class StreamMetricsGetter(object):
 
  def __init__(
    self,
    cloudwatch_client,
    kinesis_client,
    stream_name,
    start_time,
    end_time,
    statistic = 'Average',
    period = 60,
  ):
    self.cloudwatch_client = cloudwatch_client
    self.kinesis_client = kinesis_client
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
          self.statistic,
        )
      )
    return metrics_array

  def get_metric_datapoints(self, metric_name):
    response = self.cloudwatch_client.get_metric_statistics(
      Namespace = self.namespace,
      MetricName = metric_name,
      StartTime = self.start_time,
      EndTime = self.end_time,
      Statistics = [self.statistic],
      Period = self.period, 
      Dimensions = [{'Name': 'StreamName', 'Value': self.stream_name}]
    )
    return response['Datapoints']


