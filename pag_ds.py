import boto3

kinesis = boto3.client('kinesis')

def has_more_shards(stream_description):
  hms = 'HasMoreShards' in stream_description and stream_description['HasMoreShards'] == True
  print "HMS: {0}".format(hms)
  return hms

exclusive_start_shard_id = None
while True:
  describe_stream_args =  {'StreamName' : 'MetricsTest', 'Limit' : 2}
  if exclusive_start_shard_id is not None:
    describe_stream_args['ExclusiveStartShardId'] = exclusive_start_shard_id 
  stream_description = kinesis.describe_stream(**describe_stream_args)['StreamDescription']
  shards = stream_description['Shards']
  shard_array = map(lambda shard: shard['ShardId'], shards)
  if len(shard_array) > 0:
    print shard_array
  print stream_description
  more_shards = has_more_shards(stream_description)
  if more_shards == True:
    exclusive_start_shard_id = shard_array[-1]
    continue
  else:
    break
