
The pull command calls GetRecords in a loop for the specified stream and shard.

``Example 1:``

This command retrieves data from shard 0 of stream Test. GetRecords is called every 5000 ms (default).

aws kinesis pull --stream-name Test --shard-id shardId-000000000001

``Example 2:``

This command retrieves data from shard 0 of stream Test. It calls GetRecords every 500ms.

aws kinesis pull --stream-name Test --shard-id shardId-00000000000 --pull-delay 500
