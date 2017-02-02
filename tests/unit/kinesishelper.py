from mock import MagicMock, patch
from kinesis_awscli_plugin.lib.kinesishelper import KinesisHelper


def fake_has_more_shards(self, stream_description):
  TestKinesisHelper.has_more_shards_counter += 1
  if TestKinesisHelper.has_more_shards_counter < 5:
    return True
  else:
    return False


class TestKinesisHelper:

  has_more_shards_counter = 0
 
  @patch.object(KinesisHelper, 'has_more_shards', fake_has_more_shards) 
  def test_stream_shards(self):
     k = KinesisHelper(MagicMock(), MagicMock())
     k.client.describe_stream.return_value = {'StreamDescription':{'Shards':[{'ShardId': 'one'},{'ShardId': 'two'}]}}
     shard_ids = k.stream_shards('test')
     print "SHARD ID's: %s" % shard_ids
     assert len(shard_ids) == 10




