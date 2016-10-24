from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics

class KinesisMetricsGetter(object):
 
  def __init__(
    self,
    cloudwatch_client,
    kinesis_client,
    stream_name,
    start_time,
    end_time,
    metric_name = 'IncomingRecords',
    statistic = 'Average',
    period = 60,
  ):
    self.cloudwatch_client = cloudwatch_client
    self.kinesis_client = kinesis_client
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

  def get_shard_ids_for_stream(self):
    #BUG BUG - do we need to paginate or does the Python SDK?
    response = self.kinesis_client.describe_stream(
      StreamName = self.stream_name
    )
     
    shard_ids = []
    for shard in response['StreamDescription']['Shards']:
      shard_ids.append(shard['ShardId'])
    return shard_ids
  
  def get_shard_metrics(self, shard_ids):
    shard_metrics_array = []
    for shard_id in shard_ids:
      datapoints = self.get_shard_datapoints(shard_id)
      # only append if we got data:
      if len(datapoints) > 0: 
        shard_metrics_array.append(
          KinesisMetrics(
            shard_id, 
            datapoints, 
          )
        )
    return shard_metrics_array


  def get_shard_datapoints(self, shard_id):
    response = self.cloudwatch_client.get_metric_statistics(
      Namespace = self.namespace,
      MetricName = self.metric_name,
      StartTime = self.start_time,
      EndTime = self.end_time,
      Statistics = [self.statistic],
      Period = self.period, 
      Dimensions = self.get_dimensions(shard_id),
    )
    return self.metric_values(
      response['Datapoints'],
      self.statistic
    )
 
  def sort(self, shard_metrics_array):
    return  sorted(
      shard_metrics_array, 
      key=lambda _shard_metrics_array: _shard_metrics_array.avg(),
      reverse=True
    )

  def get_dimensions(self, shard_id):
    return [
      {
        'Name': 'StreamName',
        'Value': self.stream_name
      },
      {
         'Name': 'ShardId',
         'Value': shard_id
       },
    ]

  def metric_values(self, datapoints, statistic):
    return  map(lambda x: float(x[statistic]), datapoints)


