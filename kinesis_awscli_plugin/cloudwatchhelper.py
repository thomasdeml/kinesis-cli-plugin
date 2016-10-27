from kinesis_awscli_plugin.awshelper import AWSHelper

class CloudWatchHelper(AWSHelper):

  def __init__(self, session, args):
    super(CloudWatchHelper, self).__init__(session)
    self.client = self.get_generic_client('cloudwatch', args)

  def get_metric_datapoints(self, **cw_args):
    response = self.client.get_metric_statistics(**cw_args)
    return response['Datapoints']
 
  def get_shard_dimensions(self, stream_name, shard_id):
    return [
      {
        'Name': 'StreamName',
        'Value': stream_name
      },
      {
         'Name': 'ShardId',
         'Value': shard_id
       },
    ]

  def get_stream_dimensions(self, stream_name):
    return [
      {
        'Name': 'StreamName',
        'Value': stream_name
      },
    ] 
