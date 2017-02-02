from kinesis_awscli_plugin.lib.awshelper import AWSHelper


class KinesisHelper(AWSHelper):
    def __init__(self, session, args):
        super(KinesisHelper, self).__init__(session)
        self.client = self.get_generic_client('kinesis', args)

    def shard_metrics_enabled(self, stream_name):
        stream_data = self.client.describe_stream(
            StreamName=stream_name)['StreamDescription']
        return len(stream_data['EnhancedMonitoring'][0][
            'ShardLevelMetrics']) > 0

    def stream_shards(self, stream_name):
        exclusive_start_shard_id = None
        shard_array = []
        while True:
            describe_stream_args = self.create_paginated_describe_stream_args(
                stream_name, exclusive_start_shard_id)
            stream_description = self.client.describe_stream(
                **describe_stream_args)['StreamDescription']
            shards = stream_description['Shards']
            shard_array.extend(map(lambda shard: shard['ShardId'], shards))
            more_shards = self.has_more_shards(stream_description)
            if more_shards == True:
                exclusive_start_shard_id = shard_array[-1]
                continue
            else:
                break
        return shard_array

    def create_paginated_describe_stream_args(self, stream_name,
                                              exclusive_start_shard_id):
        describe_stream_args = {'StreamName': stream_name}
        if exclusive_start_shard_id is not None:
            describe_stream_args[
                'ExclusiveStartShardId'] = exclusive_start_shard_id
        return describe_stream_args

    def has_more_shards(self, stream_description):
        return 'HasMoreShards' in stream_description and stream_description[
            'HasMoreShards'] == True

    def get_shard_iterator_from_latest(self, stream_name, shard_id):
        params = dict(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST')
        gsi_response = self.client.get_shard_iterator(**params)
        if gsi_response and gsi_response['ShardIterator']:
            return gsi_response['ShardIterator']
        else:
            raise Exception(
                'GetShardIterator did not return a valid iterator for stream %s, shard %s'
                % (stream_name, shard_id))
    
    def put_record(self, stream_name, partition_key, data):
        params = dict(
            StreamName=stream_name, PartitionKey=partition_key, Data=data)
        return self.client.put_record(**params)
 
