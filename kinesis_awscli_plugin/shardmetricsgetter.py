from kinesis_awscli_plugin.kinesismetrics import KinesisMetrics

class ShardMetricsGetter(object):
 
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
    return self.paginated_describe_stream_shards()

  def paginated_describe_stream_shards(self):
    exclusive_start_shard_id = None
    shard_array = []
    while True:
      describe_stream_args = self.create_paginated_describe_stream_args(exclusive_start_shard_id)
      stream_description = self.kinesis_client.describe_stream(**describe_stream_args)['StreamDescription']
      shards = stream_description['Shards']
      shard_array.extend(map(lambda shard: shard['ShardId'], shards))
      more_shards = self.has_more_shards(stream_description)
      if more_shards == True:
        exclusive_start_shard_id = shard_array[-1]
        continue
      else:
        break
    return shard_array 

  def create_paginated_describe_stream_args(self, exclusive_start_shard_id):
    describe_stream_args =  {'StreamName' : self.stream_name}
    if exclusive_start_shard_id is not None:
      describe_stream_args['ExclusiveStartShardId'] = exclusive_start_shard_id 
    return describe_stream_args 

  def has_more_shards(self, stream_description):
    return 'HasMoreShards' in stream_description and stream_description['HasMoreShards'] == True
 
  def get_shard_metrics(self, shard_ids):
    shard_metrics_array = []
    for shard_id in shard_ids:
      shard_metrics_array.append(
        KinesisMetrics(
          shard_id, 
          self.get_shard_datapoints(shard_id),
          self.statistic
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
    return response['Datapoints']
 
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
